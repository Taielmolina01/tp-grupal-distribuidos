package filter

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type FilterConfig struct {
	Id             int
	MomHost        string
	MomPort        int
	inputExchange  string
	outputExchange string
}

type Filter[T comparable] struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	callback       func(T) bool
}

// Inicializadores

func newFilter[T comparable](config FilterConfig, callback func(T) bool) (*Filter[T], error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.inputExchange, []string{}, connSettings)

	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.outputExchange, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &Filter[T]{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		callback:       callback,
	}, nil
}

func (filter *Filter[T]) Run() {
	slog.Info("Starting filter consumers", "filter_id", filter.id)
	go filter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filter.handleMessage(msg, ack, nack)
	})
}

func (filter *Filter[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	// if filter.callback(msg.toTransferDTO()) {
	// 	filter.outputExchange.Send(msg)
	// }
}

// Handler para la working queue que comparten las distintas intancias de sum.
