package filter

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
)

// Inicializadores

func newDistinctFilter(config FilterConfig) (worker.Worker, error) {
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

	return &DistinctFilter{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
	}, nil
}

func (distinctfilter *DistinctFilter) Run() {
	slog.Info("Starting filter consumers", "filter_id", distinctfilter.id)
	if err := distinctfilter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		distinctfilter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (distinctfilter *DistinctFilter) handleMessage(msg middleware.Message, ack, nack func()) {
	// if value, _ := distinctfilter.alreadySeen[msg.toTransferDTO()]; !value {
	// 	distinctfilter.outputExchange.Send(msg)
	//  distinctfilter.alreadySeen[msg.toTransferDTO()] = true
	// }
}

func (distinctfilter *DistinctFilter) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
