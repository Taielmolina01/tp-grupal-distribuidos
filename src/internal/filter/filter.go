package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newFilter[T comparable](config FilterConfig, callback func(T) bool) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Named shared queue bound to the transfers exchange with the Q1234 routing key.
	// Multiple filter instances using this same queue compete for messages (BBB buffer).
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.InputQueue, config.InputRoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.OutputQueue, config.OutputRoutingKeys, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("while closing input exchange", "err", err)
		}
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
	if err := filter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

var cont int

func (filter *Filter[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	ack()

	result, err := inner.DeserializeData[T](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if filter.callback(result.Payload) {
		slog.Info("Msg passed filter", "payload", result.Payload)
		filter.outputExchange.Send(msg)
	}
}

func (filter *Filter[T]) HandleSignals() {

}

func (filter *Filter[T]) Close() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
