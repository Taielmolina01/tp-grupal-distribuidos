package filteraccountseen

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/accountid"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewFilterAccountSeen(config FilterAccountSeenConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			if outputMiddleware != nil {
				if err := outputMiddleware.Close(); err != nil {
					slog.Error("While closing output middleware", "err", err)
				}
			}
			if inputMiddleware != nil {
				if err := inputMiddleware.Close(); err != nil {
					slog.Error("While closing input middleware", "err", err)
				}
			}
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)

	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewQueueMiddleware(connSettings, config.OutputMiddleware)
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			tracker:      sendertracker.New(10_000_000),
			seenAccounts: map[account.AccountIdentifier]struct{}{},
			builder:      batch.NewBuilder(config.MaxBatchSize, config.MaxBatchBytes, records.Query4ResultCodec),
		}
	})
	for clientID, state := range recovered {
		state.builder = batch.NewBuilder(config.MaxBatchSize, config.MaxBatchBytes, records.Query4ResultCodec)
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID, "seenAccounts", len(state.seenAccounts))
	}

	return &FilterAccountSeen{
		id:                   config.Id,
		queryID:              config.QueryID,
		inputMiddleware:      inputMiddleware,
		outputMiddleware:     outputMiddleware,
		states:               states,
		expectedEOFs:         config.ExpectedEOFs,
		maxBatchSize:         config.MaxBatchSize,
		maxBatchBytes:        config.MaxBatchBytes,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (f *FilterAccountSeen) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsumingBatch(f.persistBatchSize, f.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		f.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (f *FilterAccountSeen) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	f.stopConsuming()
}

func (f *FilterAccountSeen) stopConsuming() {
	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAccountSeen) close() {
	if err := f.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "filter_id", f.id, "err", err)
	}
	if err := f.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "filter_id", f.id, "err", err)
	}
}

func (f *FilterAccountSeen) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := accountid.Read(msg.Body)
		if err != nil {
			slog.Error("While deserializing input batch", "err", err)
			continue
		}

		clientID := input.ClientID
		state := f.states.For(clientID)

		if state.tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("Discarding duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			state.tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			state.tracker.RegisterBatch(int(input.SenderID))
			for i := range input.Records {
				if err := f.addRecord(clientID, state, input.Records[i]); err != nil {
					slog.Error("While sending results batch", "err", err)
					nack()
					f.stopConsuming()
					return
				}
			}
		}

		state.tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if state.tracker.IsComplete(f.expectedEOFs) {
			if !state.builder.IsEmpty() {
				if err := f.flushResults(clientID, state); err != nil {
					slog.Error("While flushing results on completion", "err", err)
					nack()
					f.stopConsuming()
					return
				}
			}
			eofBody := batch.WriteEOF(clientID, uint8(f.queryID), 0, 0, input.Total)
			if err := f.outputMiddleware.Send(newmiddleware.Message{Body: eofBody}); err != nil {
				slog.Error("While sending EOF message", "err", err)
				nack()
				f.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			f.states.Delete(clientID)
			f.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := f.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			f.stopConsuming()
			return
		}
	}

	ack()
}

func (f *FilterAccountSeen) addRecord(clientID int, state *clientState, record account.AccountIdentifier) error {
	if _, ok := state.seenAccounts[record]; ok {
		return nil
	}

	// TODO: Con la nueva lógica de propagr el SeqNum acá tenemos que acumular hasta el final y solo ahí mandar todos los outputs. Tampoco nos es muy caro así q np
	state.seenAccounts[record] = struct{}{}
	result := queryresult.Query4Result{
		BankId:        record.BankID,
		AccountNumber: record.AccountNumber,
	}
	if !state.builder.TryAdd(&result) {
		if err := f.flushResults(clientID, state); err != nil {
			return err
		}
		state.builder.TryAdd(&result)
	}
	return nil
}

func (f *FilterAccountSeen) flushResults(clientID int, state *clientState) error {
	body := state.builder.Flush(clientID, uint8(f.queryID), 0, 0)
	return f.outputMiddleware.Send(newmiddleware.Message{Body: body})
}
