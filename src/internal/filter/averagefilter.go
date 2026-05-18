package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/worker"
)

func newAverageFilter(config FilterConfig, compareFunc func(float32, float32) bool) (worker.Worker, error) {
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

	return &AverageFilter{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		compareFunc:    compareFunc,
	}, nil
}

func (averageFilter *AverageFilter) Run() {
	slog.Info("Starting filter consumers", "filter_id", averageFilter.id)
	if err := averageFilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		averageFilter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (averageFilter *AverageFilter) handleMessage(msg middleware.Message, ack, nack func()) {
	// TO DO: implementar esto
	// Debería:
	// - Consumir de la queue de transferencias crudas. Si no tengo el promedio guardo en un file la transferencia hasta que la pueda
	// consumir. En cambio si lo tengo, simplemente paso por la función de comparación y si pasa la comparación, la mando a la queue de transferencias filtradas.
	// - Consumir de la queue de promedios. Cuando tenga un promedio guardado, consumo las transferencias crudas que tengo guardadas y
	// las comparo con el promedio usando la función de comparación. Si pasan la comparación, las mando a la queue de transferencias filtradas.
}

func (averageFilter *AverageFilter) HandleSignals() {

}

func (averageFilter *AverageFilter) Close() {

}
