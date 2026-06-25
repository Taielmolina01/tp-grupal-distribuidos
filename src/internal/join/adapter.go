package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewJoin(config JoinConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	legacyConn := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		leftInput  newmiddleware.Middleware
		rightInput newmiddleware.Middleware
		output     middleware.Middleware
		err        error
	)

	defer func() {
		if err != nil {
			cleanup.Close(leftInput, rightInput, output)
		}
	}()

	leftInput, err = newmiddleware.NewQueueMiddleware(connSettings, config.LeftInputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating left input middleware: %w", err)
	}

	rightInput, err = newmiddleware.NewQueueMiddleware(connSettings, config.RightInputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating right input middleware: %w", err)
	}

	output, err = middleware.CreateQueueMiddleware(config.OutputQueue, legacyConn)
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
			leftBuffer:   map[string]transfer.TransferForQ2{},
			rightBuffer:  map[string]account.Account{},
			leftTracker:  sendertracker.New(10_000_000),
			rightTracker: sendertracker.New(10_000_000),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &Join{
		id:                   config.Id,
		queryID:              config.QueryID,
		leftInput:            leftInput,
		rightInput:           rightInput,
		output:               output,
		leftEofsExpected:     config.LeftEofsExpected,
		rightEofsExpected:    config.RightEofsExpected,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (j *Join) Run() {
	defer j.close()
	go j.consumeRight()

	if err := j.leftInput.StartConsumingBatch(j.persistBatchSize, j.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		j.handleLeftBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from left input", "err", err)
	}
}

func (j *Join) consumeRight() {
	if err := j.rightInput.StartConsumingBatch(j.persistBatchSize, j.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		j.handleRightBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from right input", "err", err)
	}
}

func (j *Join) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumers")
	j.stopConsuming()
}

func (j *Join) stopConsuming() {
	if err := j.leftInput.StopConsuming(); err != nil {
		slog.Error("While stopping left input consumer", "err", err)
	}
	if err := j.rightInput.StopConsuming(); err != nil {
		slog.Error("While stopping right input consumer", "err", err)
	}
}

func (j *Join) close() {
	cleanup.Close(j.leftInput, j.rightInput, j.output)
}

func (j *Join) handleLeftBatch(msgs []newmiddleware.Message, ack, nack func()) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.handleBatch(msgs, ack, nack, true)
}

func (j *Join) handleRightBatch(msgs []newmiddleware.Message, ack, nack func()) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.handleBatch(msgs, ack, nack, false)
}

func (j *Join) handleBatch(msgs []newmiddleware.Message, ack, nack func(), isLeft bool) {
	modified := make(map[int]*clientState)

	for _, msg := range msgs {
		clientID, ok := j.processMessage(msg, isLeft)
		if !ok {
			continue
		}
		modified[clientID] = j.states.For(clientID)
	}

	for clientID, state := range modified {
		if err := j.maybeEmit(clientID, state); err != nil {
			slog.Error("emit failed, stopping", "err", err)
			nack()
			j.stopConsuming()
			return
		}
		if err := j.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			j.stopConsuming()
			return
		}
	}

	ack()
}
