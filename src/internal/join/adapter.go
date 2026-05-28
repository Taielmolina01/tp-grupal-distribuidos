package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

type TwoInputAdapter[L, R, O any] struct {
	id                int
	joinAmount        int
	queryID           uint8
	join              *Join[L, R, O]
	leftInput         middleware.Middleware
	rightInput        middleware.Middleware
	output            middleware.Middleware
	leftEofCount      map[int]int
	rightEofCount     map[int]int
	leftEofsExpected  int
	rightEofsExpected int
	fired             map[int]bool
	lock              sync.Mutex
}

func newTwoInputJoin[L, R, O any](
	config JoinConfig,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
	leftCombine func(L, L) L,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	leftInput, err := middleware.CreateQueueMiddleware(config.LeftInputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	rightInput, err := middleware.CreateQueueMiddleware(config.RightInputQueue, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		return nil, err
	}

	output, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		if err := rightInput.Close(); err != nil {
			slog.Error("while closing right input", "err", err)
		}
		return nil, err
	}

	slog.Info("join started",
		"left_queue", config.LeftInputQueue,
		"right_queue", config.RightInputQueue,
		"output_queue", config.OutputQueue,
	)

	leftEofs := config.LeftEofsExpected
	if leftEofs <= 0 {
		leftEofs = 1
	}
	rightEofs := config.RightEofsExpected
	if rightEofs <= 0 {
		rightEofs = 1
	}

	adapter := &TwoInputAdapter[L, R, O]{
		id:                config.Id,
		joinAmount:        config.Amount,
		queryID:           config.QueryID,
		join:              newJoin(output, leftKey, rightKey, combine, leftCombine, config.QueryID),
		leftInput:         leftInput,
		rightInput:        rightInput,
		output:            output,
		leftEofCount:      map[int]int{},
		rightEofCount:     map[int]int{},
		leftEofsExpected:  leftEofs,
		rightEofsExpected: rightEofs,
		fired:             map[int]bool{},
	}

	return adapter, nil
}

func (a *TwoInputAdapter[L, R, O]) Run() {
	done := make(chan struct{})
	go func() {
		if err := a.leftInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			data, err := inner.DeserializeData[L](&msg)
			if err != nil {
				slog.Error("while deserializing left message", "err", err)
				return
			}
			if data.IsEOF() {
				a.lock.Lock()
				a.leftEofCount[data.ClientID]++
				a.lock.Unlock()
				a.handleEOF(data.ClientID)
				return
			}
			a.join.HandleLeft(data.ClientID, data.Payload)
		}); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := a.rightInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		data, err := inner.DeserializeData[R](&msg)
		if err != nil {
			slog.Error("while deserializing right message", "err", err)
			return
		}
		if data.IsEOF() {
			a.lock.Lock()
			a.rightEofCount[data.ClientID]++
			a.lock.Unlock()
			a.handleEOF(data.ClientID)
			return
		}
		a.join.HandleRight(data.ClientID, data.Payload)
	}); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (a *TwoInputAdapter[L, R, O]) handleEOF(clientID int) {
	a.lock.Lock()
	if a.fired[clientID] {
		a.lock.Unlock()
		return
	}
	if a.leftEofCount[clientID] < a.leftEofsExpected || a.rightEofCount[clientID] < a.rightEofsExpected {
		a.lock.Unlock()
		return
	}
	a.fired[clientID] = true
	delete(a.leftEofCount, clientID)
	delete(a.rightEofCount, clientID)
	a.lock.Unlock()

	a.join.HandleQueryEOF(clientID)

	msgOut, err := inner.SerializeData(inner.DataMsg[any]{
		ClientID: clientID,
		EOF:      &inner.EOFInfo{Kind: "commit"},
		QueryID:  a.queryID,
	})
	if err != nil {
		slog.Error("while serializing join EOF", "err", err)
		return
	}
	if err := a.output.Send(*msgOut); err != nil {
		slog.Error("while sending join EOF downstream", "err", err)
	}
}

func (a *TwoInputAdapter[L, R, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	if err := a.leftInput.StopConsuming(); err != nil {
		slog.Error("while stopping left input", "err", err)
	}
	if err := a.rightInput.StopConsuming(); err != nil {
		slog.Error("while stopping right input", "err", err)
	}
	if err := a.leftInput.Close(); err != nil {
		slog.Error("while closing left input", "err", err)
	}
	if err := a.rightInput.Close(); err != nil {
		slog.Error("while closing right input", "err", err)
	}
	if err := a.join.output.Close(); err != nil {
		slog.Error("while closing output", "err", err)
	}
}
