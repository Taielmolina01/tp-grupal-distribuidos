package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

// Inicializadores

func newDistinctFilter[T comparable](config FilterConfig, compareFunc func(T, T) bool) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, []string{}, connSettings)

	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, []string{}, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("while closing input exchange", "err", err)
		}
		return nil, err
	}

	return &DistinctFilter[T]{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		compareFunc:    compareFunc,
	}, nil
}

func (distinctfilter *DistinctFilter[T]) Run() {
	slog.Info("Starting filter consumers", "filter_id", distinctfilter.id)
	if err := distinctfilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		distinctfilter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	// Acá cuando no usemos más el mapa en memoria deberiamos usar la funcion de comparación
	// leyendo el archivo donde tenemos las ya guardadas.

	// if value, _ := distinctfilter.alreadySeen[msg.toTransferDTO()]; !value {
	// 	distinctfilter.outputExchange.Send(msg)
	// 	distinctfilter.alreadySeen[msg.toTransferDTO()] = true
	// }
}

func (distinctfilter *DistinctFilter[T]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
