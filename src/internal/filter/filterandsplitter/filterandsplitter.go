package filterandsplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/splittransfer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewFilterAndSplitter(config FilterAndSplitterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			cleanup.Close(outputMiddleware, inputMiddleware)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err = newmiddleware.NewShardedMiddleware(
		connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
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
			tracker:       sendertracker.New(10_000_000),
			outputTracker: outputtracker.New(),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("DEBUG recovered client state",
			"clientID", clientID,
			"processed", state.tracker.GetMsgCount(),
			"expected", state.tracker.TotalInput(),
		)
	}

	node := &FilterAndSplitter{
		id:                   config.Id,
		startDate:            config.StartDate,
		endDate:              config.EndDate,
		outputAmount:         config.OutputMiddlewareAmount,
		prevNodeAmt:          config.ExpectedEOFs,
		hasher:               shard.New(config.OutputMiddlewareAmount),
		queryID:              config.QueryID,
		inputMiddleware:      inputMiddleware,
		outputMiddleware:     outputMiddleware,
		checkpoint:           ckpt,
		states:               states,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}

	return node, nil
}

func (f *FilterAndSplitter) Run() {
	defer f.close()

	if err := f.inputMiddleware.StartConsumingBatch(f.persistBatchSize, f.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		f.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (f *FilterAndSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	f.stopConsuming()
}

func (f *FilterAndSplitter) close() {
	cleanup.Close(f.inputMiddleware, f.outputMiddleware)
}

func (f *FilterAndSplitter) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})
	aborted := make(map[int]bool)

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := f.states.For(clientID)
		tracker := state.tracker

		if input.Abort || aborted[clientID] {
			aborted[clientID] = true
			modified[clientID] = state
			continue
		}

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			if err := f.processBatch(input, state); err != nil {
				slog.Error("process batch failed", "err", err)
				nack()
				f.stopConsuming()
				return
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(f.prevNodeAmt) {
			if err := f.finishTransfersStep(clientID, state); err != nil {
				slog.Error("finishing transfers step failed", "err", err)
				nack()
				f.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if aborted[clientID] {
			if err := msgsend.SendAbort(f.outputMiddleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
				slog.Error("While emitting abort", "err", err)
				nack()
				f.stopConsuming()
				return
			}
			f.states.Delete(clientID)
			f.checkpoint.DeleteClient(clientID)
			continue
		}
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

func (f *FilterAndSplitter) stopConsuming() {
	if err := f.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (f *FilterAndSplitter) processBatch(input batch.Msg[transfer.TransferAfterCurrency], state *clientState) error {
	clientID := input.ClientID
	byShard := make(map[string][]transfer.SplittedTransfer)

	for _, record := range input.Records {
		if record.Timestamp.Before(f.startDate) || record.Timestamp.After(f.endDate) {
			continue
		}

		projected := transfer.ProjectForQ4(record)
		for _, o := range []transfer.SplittedTransfer{
			{Transfer: projected, IsLeftPart: true},
			{Transfer: projected, IsLeftPart: false},
		} {
			var bank, acc string
			if o.IsLeftPart {
				bank, acc = o.Transfer.FromBank, o.Transfer.FromBankAccount
			} else {
				bank, acc = o.Transfer.ToBank, o.Transfer.ToBankAccount
			}
			routingKey := fmt.Sprintf("shard-%d", f.hasher.ShardFor(clientID, bank, acc))
			byShard[routingKey] = append(byShard[routingKey], o)
		}
	}

	for routingKey, group := range byShard {
		body := splittransfer.WriteBatch(clientID, f.queryID, uint8(f.id), input.Seq, group)
		if err := f.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: routingKey}); err != nil {
			return err
		}
		state.outputTracker.RegisterBatch(routingKey)
	}
	return nil
}

func (f *FilterAndSplitter) finishTransfersStep(clientID int, state *clientState) error {
	eofSeq := state.tracker.GetEOFSeq()
	var sendErr error
	for i := range f.outputAmount {
		if sendErr != nil {
			break
		}
		rk := fmt.Sprintf("shard-%d", i)
		total := state.outputTracker.CountFor(rk)
		if err := msgsend.SendEOF(f.outputMiddleware, rk, clientID, f.queryID, uint8(f.id), eofSeq, uint32(total)); err != nil {
			slog.Error("While sending EOF", "routingKey", rk, "err", err)
			sendErr = err
		}
	}
	return sendErr
}
