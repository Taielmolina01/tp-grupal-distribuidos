package filter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

type CountAndFilterConfig struct {
	Type           FilterType
	Id             int
	MomHost        string
	MomPort        int
	InputExchange  string
	OutputExchange string
	AmountTreshold int
}

type CountAndFilter[T comparable] struct {
	id             uint32
	inputExchange  newmiddleware.Middleware
	outputExchange newmiddleware.Middleware
	amountTreshold int
	countsByKey    map[T]int
}

func newCountAndFilter[T comparable](config CountAndFilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := newmiddleware.NewFanoutMiddleware(connSettings, config.InputExchange, "")

	if err != nil {
		return nil, err
	}

	outputExchange, err := newmiddleware.NewFanoutMiddleware(connSettings, config.OutputExchange, "")
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("while closing input exchange", "err", err)
		}
		return nil, err
	}

	return &CountAndFilter[T]{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		amountTreshold: config.AmountTreshold,
		countsByKey:    map[T]int{},
	}, nil
}

func (cf *CountAndFilter[T]) Run() {
	slog.Info("Starting count and filter consumers", "filter_id", cf.id)
	if err := cf.inputExchange.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		cf.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (cf *CountAndFilter[T]) handleMessage(msg newmiddleware.Message, ack, nack func()) {
	defer ack()

	// data := inner.DesarializeMessage(msg)

	// if cf.countsByKey[data.transferId] < cf.amountTreshold {
	// 	cf.countsByKey[data] += 1
	// 	if cf.countsByKey[data.transferId] == cf.amountTreshold {
	// 		cf.outputExchange.Send(msg)
	// 	}
	// }
}

func (cf *CountAndFilter[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := cf.Close(); err != nil {
		slog.Error("While closing countAndFitler node", "err", err)
	}
}

func (cf *CountAndFilter[T]) Close() error {
	if err := cf.inputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := cf.inputExchange.Close(); err != nil {
		return err
	}
	if err := cf.outputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := cf.outputExchange.Close(); err != nil {
		return err
	}
	clear(cf.countsByKey)
	return nil
}
