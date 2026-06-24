package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newCountReducer(config ReducerConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	legacyConn := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := newmiddleware.NewQueueMiddleware(connSettings, config.InputQueue)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueues[0], legacyConn)
	if err != nil {
		_ = inputQueue.Close()
		return nil, err
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalCountClientState, unmarshalCountClientState)
	if err != nil {
		_ = inputQueue.Close()
		_ = outputQueue.Close()
		return nil, err
	}

	recovered, err := ckpt.Load()
	if err != nil {
		_ = inputQueue.Close()
		_ = outputQueue.Close()
		return nil, err
	}

	states := statemap.New(func() *countClientState {
		return &countClientState{
			tracker: sendertracker.New(10_000_000),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &CountReducer{
		id:                   config.Id,
		queryID:              config.QueryID,
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		prevNodeAmt:          config.InputEofsExpected,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (r *CountReducer) Run() {
	defer r.close()
	if err := r.inputQueue.StartConsumingBatch(r.persistBatchSize, r.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		r.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (r *CountReducer) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	r.stopConsuming()
}

func (r *CountReducer) stopConsuming() {
	if err := r.inputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (r *CountReducer) close() {
	if err := r.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := r.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (r *CountReducer) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
	modified := make(map[int]*countClientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, records.FinalTransferForQ5Codec)
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
			state.count += uint32(len(input.Records))
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

func (r *CountReducer) finishStep(clientID int, state *countClientState) error {
	total := state.count
	slog.Info("all EOFs received, sending result",
		"clientID", clientID,
		"totalCount", total,
	)

	resultBody := batch.Write(
		clientID,
		r.queryID,
		uint8(r.id),
		0,
		[]queryresult.Query5Result{{Qty: total}},
		records.Query5ResultCodec,
	)
	if err := r.outputQueue.Send(middleware.Message{Body: resultBody}); err != nil {
		return err
	}

	eofBody := batch.WriteEOF(clientID, r.queryID, uint8(r.id), 0, 1)
	if err := r.outputQueue.Send(middleware.Message{Body: eofBody}); err != nil {
		return err
	}

	return nil
}
