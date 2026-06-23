package reducer

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newReducer(
	config ReducerConfig,
	reducerFunction func(transfer.TransferAfterCurrency, transfer.TransferAfterCurrency) transfer.TransferAfterCurrency,
	keyFunc func(transfer.TransferAfterCurrency) string,
	projectFunc func(transfer.TransferAfterCurrency) transfer.TransferForQ2,
	outputCodec wire.Codec[transfer.TransferForQ2],
) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	legacyConn := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware newmiddleware.Middleware
		err             error
	)
	outputQueues := make([]middleware.Middleware, 0, len(config.OutputQueues))

	defer func() {
		if err != nil {
			if inputMiddleware != nil {
				if closeErr := inputMiddleware.Close(); closeErr != nil {
					slog.Error("While closing input middleware", "err", closeErr)
				}
			}
			for _, q := range outputQueues {
				if closeErr := q.Close(); closeErr != nil {
					slog.Error("While closing output queue", "err", closeErr)
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

	for _, name := range config.OutputQueues {
		var m middleware.Middleware
		m, err = middleware.CreateQueueMiddleware(name, legacyConn)
		if err != nil {
			return nil, fmt.Errorf("creating output queue %s: %w", name, err)
		}
		outputQueues = append(outputQueues, m)
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
			tracker:   sendertracker.New(10_000_000),
			maxByBank: map[string]transfer.TransferAfterCurrency{},
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &Reducer{
		id:                   config.Id,
		queryID:              config.QueryID,
		inputMiddleware:      inputMiddleware,
		outputQueues:         outputQueues,
		prevNodeAmt:          config.ExpectedEOFs,
		reducerFunction:      reducerFunction,
		keyFunc:              keyFunc,
		projectFunc:          projectFunc,
		outputCodec:          outputCodec,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (r *Reducer) Run() {
	defer r.close()
	if err := r.inputMiddleware.StartConsumingBatch(r.persistBatchSize, r.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		r.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (r *Reducer) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	r.stopConsuming()
}

func (r *Reducer) stopConsuming() {
	if err := r.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (r *Reducer) close() {
	if err := r.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "err", err)
	}
	for _, q := range r.outputQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing output queue", "err", err)
		}
	}
}

func (r *Reducer) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := r.states.For(clientID)
		tracker := state.tracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			r.processRecords(input.Records, state)
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(r.prevNodeAmt) {
			if err := r.finishStep(clientID, state); err != nil {
				slog.Error("finish step failed", "err", err)
				nack()
				r.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			r.states.Delete(clientID)
			r.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := r.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			r.stopConsuming()
			return
		}
	}

	ack()
}

func (r *Reducer) processRecords(recordsBatch []transfer.TransferAfterCurrency, state *clientState) {
	for i := range recordsBatch {
		rec := recordsBatch[i]
		key := r.keyFunc(rec)
		existing, ok := state.maxByBank[key]
		if !ok {
			state.maxByBank[key] = rec
		} else {
			state.maxByBank[key] = r.reducerFunction(existing, rec)
		}
	}
}

func (r *Reducer) finishStep(clientID int, state *clientState) error {
	byShard := make(map[int][]transfer.TransferForQ2)
	for _, v := range state.maxByBank {
		idx := shard.CalculateIndexForShard(clientID, r.keyFunc(v), len(r.outputQueues))
		byShard[idx] = append(byShard[idx], r.projectFunc(v))
	}

	for idx, group := range byShard {
		body := batch.Write(clientID, r.queryID, 0, 0, group, r.outputCodec)
		if err := r.outputQueues[idx].Send(middleware.Message{Body: body}); err != nil {
			return err
		}
	}

	eofBody := batch.WriteEOF(clientID, r.queryID, 0, 0, 0)
	for _, q := range r.outputQueues {
		if err := q.Send(middleware.Message{Body: eofBody}); err != nil {
			return err
		}
	}
	return nil
}
