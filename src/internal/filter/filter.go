package filter

import (
	"log/slog"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
)

type FilterType int

const (
	CURRENCY FilterType = iota
	AMOUNT
	DATE_RANGE
	DATE_RANGE_AND_PAYMENT
)

type FilterConfig struct {
	Type           FilterType
	Id             int
	MomHost        string
	MomPort        int
	InputExchange  string
	OutputExchange string
	// parameters used by constructors
	Currency       string
	Amount         float32
	StartDateRange time.Time
	EndDateRange   time.Time
	Currencies     []string
}

type Filter[T comparable] struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	callback       func(T) bool
}

// Inicializadores

func newFilter[T comparable](config FilterConfig, callback func(T) bool) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, []string{}, connSettings)

	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, []string{}, connSettings)
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

func (filter *Filter[T]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
