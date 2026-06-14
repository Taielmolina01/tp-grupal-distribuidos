package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
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
	leftCodec         wire.Codec[L]
	rightCodec        wire.Codec[R]
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
	leftCodec wire.Codec[L],
	rightCodec wire.Codec[R],
	outputCodec wire.Codec[O],
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
		join:              newJoin(output, outputCodec, leftKey, rightKey, combine, leftCombine, config.QueryID),
		leftInput:         leftInput,
		rightInput:        rightInput,
		output:            output,
		leftCodec:         leftCodec,
		rightCodec:        rightCodec,
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
	defer a.Close()
	go func() {
		if err := a.leftInput.StartConsuming(a.HandleLeft); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := a.rightInput.StartConsuming(a.HandleRight); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
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
}

// Consuming handlers

func (a *TwoInputAdapter[L, R, O]) HandleLeft(msg middleware.Message, ack, nack func()) {
	defer ack()

	input, err := batch.Read(msg.Body, a.leftCodec)
	if err != nil {
		slog.Error("while deserializing left batch", "err", err)
		return
	}
	if input.EOF {
		a.HandleLeftEof(input.ClientID)
		a.handleEOF(input.ClientID)
		return
	}
	for i := range input.Records {
		a.join.HandleLeft(input.ClientID, input.Records[i])
	}
}

func (a *TwoInputAdapter[L, R, O]) HandleRight(msg middleware.Message, ack, nack func()) {
	defer ack()

	input, err := batch.Read(msg.Body, a.rightCodec)
	if err != nil {
		slog.Error("while deserializing right batch", "err", err)
		return
	}
	if input.EOF {
		a.HandleRightEof(input.ClientID)
		a.handleEOF(input.ClientID)
		return
	}
	for i := range input.Records {
		a.join.HandleRight(input.ClientID, input.Records[i])
	}
}

// Helpers with locking for EOF handling

func (a *TwoInputAdapter[L, R, O]) HandleLeftEof(clientID int) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.leftEofCount[clientID]++
}

func (a *TwoInputAdapter[L, R, O]) HandleRightEof(clientID int) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.rightEofCount[clientID]++
}
func (a *TwoInputAdapter[L, R, O]) handleEOF(clientID int) {
	if !a.handleEofWithLock(clientID) {
		return
	}

	a.join.HandleQueryEOF(clientID)

	eofBody := batch.WriteEOF(clientID, a.queryID, 0, 0, 0)
	if err := a.output.Send(middleware.Message{Body: eofBody}); err != nil {
		slog.Error("while sending join EOF downstream", "err", err)
	}
}

func (a *TwoInputAdapter[L, R, O]) handleEofWithLock(clientId int) bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.fired[clientId] || a.leftEofCount[clientId] < a.leftEofsExpected || a.rightEofCount[clientId] < a.rightEofsExpected {
		return false
	}
	a.fired[clientId] = true
	delete(a.leftEofCount, clientId)
	delete(a.rightEofCount, clientId)

	return true
}

// Close helper

func (a *TwoInputAdapter[L, R, O]) Close() {
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
