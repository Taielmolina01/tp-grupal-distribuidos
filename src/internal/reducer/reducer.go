package reducer

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
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
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

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			cleanup.Close(inputMiddleware, outputMiddleware)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
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
		outputMiddleware:     outputMiddleware,
		outputAmount:         config.OutputAmount,
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
	cleanup.Close(r.inputMiddleware, r.outputMiddleware)
}

func (r *Reducer) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
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
		state := r.states.For(clientID)
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
		// TODO Migrar a new middleware
		// if aborted[clientID] {
		// 	if err := msgsend.SendAbort(r.outputMiddleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
		// 		slog.Error("While emitting abort", "err", err)
		// 		nack()
		// 		r.stopConsuming()
		// 		return
		// 	}
		// 	r.states.Delete(clientID)
		// 	r.checkpoint.DeleteClient(clientID)
		// 	continue
		// }
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
	ot := outputtracker.New()

	byShard := make(map[int][]transfer.TransferForQ2)
	for _, v := range state.maxByBank {
		idx := shard.CalculateIndexForShard(clientID, r.keyFunc(v), r.outputAmount)
		byShard[idx] = append(byShard[idx], r.projectFunc(v))
	}

	for idx, group := range byShard {
		rk := fmt.Sprintf("shard-%d", idx)
		seq := ot.RegisterBatch(rk)
		body := batch.Write(clientID, r.queryID, uint8(r.id), seq, group, r.outputCodec)
		if err := r.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			return err
		}
	}

	for i := range r.outputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := ot.CountFor(rk)
		seq := ot.RegisterBatch(rk)
		eofBody := batch.WriteEOF(clientID, r.queryID, uint8(r.id), seq, uint32(total))
		if err := r.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			return err
		}
	}
	return nil
}
