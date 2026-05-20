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
	join       *Join[L, R, O]
	leftInput  middleware.Middleware
	rightInput middleware.Middleware
	eofCount   map[int]int
	lock       sync.Mutex
}

func newTwoInputJoin[L, R, O any](
	config JoinConfig,
	leftKey func(L) string,
	rightKey func(R) string,
	combine func(L, R) O,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	leftInput, err := middleware.CreateExchangeMiddleware(config.LeftInputExchange, "", []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	rightInput, err := middleware.CreateExchangeMiddleware(config.RightInputExchange, "", []string{}, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		return nil, err
	}

	output, err := middleware.CreateExchangeMiddleware(config.OutputExchange, "", []string{}, connSettings)
	if err != nil {
		if err := leftInput.Close(); err != nil {
			slog.Error("while closing left input", "err", err)
		}
		if err := rightInput.Close(); err != nil {
			slog.Error("while closing right input", "err", err)
		}
		return nil, err
	}

	return &TwoInputAdapter[L, R, O]{
		join:       newJoin[L, R, O](output, leftKey, rightKey, combine, config.QueryID),
		leftInput:  leftInput,
		rightInput: rightInput,
		eofCount:   map[int]int{},
	}, nil
}

func (a *TwoInputAdapter[L, R, O]) Run() {
	done := make(chan struct{})

	go func() {
		if err := a.leftInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			env, err := inner.DeserializeData[L](&msg)
			if err != nil {
				slog.Error("while deserializing left message", "err", err)
				return
			}
			if env.IsEOF() {
				a.handleEOF(env.ClientID)
				return
			}
			a.join.HandleLeft(env.ClientID, env.Payload)
		}); err != nil {
			slog.Error("while consuming left input", "err", err)
		}
		close(done)
	}()

	if err := a.rightInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		env, err := inner.DeserializeData[R](&msg)
		if err != nil {
			slog.Error("while deserializing right message", "err", err)
			return
		}
		if env.IsEOF() {
			a.handleEOF(env.ClientID)
			return
		}
		a.join.HandleRight(env.ClientID, env.Payload)
	}); err != nil {
		slog.Error("while consuming right input", "err", err)
	}

	<-done
}

func (a *TwoInputAdapter[L, R, O]) handleEOF(clientID int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.eofCount[clientID]++
	if a.eofCount[clientID] == 2 {
		delete(a.eofCount, clientID)
		a.join.HandleQueryEOF(clientID)
	}
}

type SingleInputAdapter[T, O any] struct {
	join   *Join[T, T, O]
	input  middleware.Middleware
	isLeft func(T) bool
}

func newSingleInputJoin[T, O any](
	config JoinConfig,
	isLeft func(T) bool,
	leftKey func(T) string,
	rightKey func(T) string,
	combine func(T, T) O,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	input, err := middleware.CreateExchangeMiddleware(config.InputExchange, "", []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	output, err := middleware.CreateExchangeMiddleware(config.OutputExchange, "", []string{}, connSettings)
	if err != nil {
		if err := input.Close(); err != nil {
			slog.Error("while closing input", "err", err)
		}
		return nil, err
	}

	return &SingleInputAdapter[T, O]{
		join:   newJoin[T, T, O](output, leftKey, rightKey, combine, config.QueryID),
		input:  input,
		isLeft: isLeft,
	}, nil
}

func (a *SingleInputAdapter[T, O]) Run() {
	if err := a.input.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		defer ack()
		env, err := inner.DeserializeData[T](&msg)
		if err != nil {
			slog.Error("while deserializing message", "err", err)
			return
		}
		if env.IsEOF() {
			a.join.HandleQueryEOF(env.ClientID)
			return
		}
		if a.isLeft(env.Payload) {
			a.join.HandleLeft(env.ClientID, env.Payload)
		} else {
			a.join.HandleRight(env.ClientID, env.Payload)
		}
	}); err != nil {
		slog.Error("while consuming input", "err", err)
	}
}

func (a *SingleInputAdapter[T, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := a.input.StopConsuming(); err != nil {
		slog.Error("while stopping input", "err", err)
	}
	if err := a.input.Close(); err != nil {
		slog.Error("while closing input", "err", err)
	}
	if err := a.join.output.Close(); err != nil {
		slog.Error("while closing output", "err", err)
	}
}

func (a *TwoInputAdapter[L, R, O]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
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
