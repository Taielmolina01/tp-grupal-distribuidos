package reducer

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

type ReducerConfig struct {
	MomHost        string
	MomPort        int
	InputExchange  string
	OutputExchange string
}

type Reducer[T comparable] struct {
	InputExchange  middleware.Middleware
	OutputExchange middleware.Middleware
	ActualValue    *T
	Callback       func(T, T) T
}

func newReducer[T comparable](config ReducerConfig, callback func(T, T) T) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, "", []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, "", []string{}, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("While closing input queue", "err", err)
		}
		return nil, err
	}

	return &Reducer[T]{
			InputExchange:  inputExchange,
			OutputExchange: outputExchange,
			ActualValue:    nil,
			Callback:       callback,
		},
		nil
}

func (reducer *Reducer[T]) Run() {
	if err := reducer.InputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		reducer.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (reducer *Reducer[T]) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	// data := inner.DeserializeMessage(msg)

	// if reducer.ActualValue == nil {
	// 	reducer.ActualValue = data.AmountPaid
	// } else {
	// 	reducer.ActualValue = reducer.Callback(reducer.ActualValue, data.AmountPaid)
	// }
}

func (reducer *Reducer[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := reducer.Close(); err != nil {
		slog.Error("While closing reducer node", "err", err)
	}
}

func (reducer *Reducer[T]) Close() error {
	if err := reducer.InputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := reducer.InputExchange.Close(); err != nil {
		return err
	}
	if err := reducer.OutputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := reducer.OutputExchange.Close(); err != nil {
		return err
	}
	return nil
}
