package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

// Inicializadores

func newFilterAndSplitter(config FilterConfig, filterFunc func(transfer.Transfer) bool, splitFunc func(transfer.Transfer) (transfer.SplittedTransfer, transfer.SplittedTransfer)) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, "", []string{}, connSettings)

	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, "", []string{}, connSettings)
	if err != nil {
		if err := inputExchange.Close(); err != nil {
			slog.Error("while closing input exchange", "err", err)
		}
		return nil, err
	}

	return &FilterAndSplitter{
		id:             uint32(config.Id),
		inputExchange:  inputExchange,
		outputExchange: outputExchange,
		filterFunction: filterFunc,
		splitFunction:  splitFunc,
	}, nil
}

func (filterAndSplitter *FilterAndSplitter) Run() {
	slog.Info("Starting filter and splitter consumers", "filter_splitter_id", filterAndSplitter.id)
	if err := filterAndSplitter.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		filterAndSplitter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (filterAndSplitter *FilterAndSplitter) handleMessage(msg middleware.Message, ack, nack func()) {
	// if filter.callback(msg.toTransferDTO()) {
	//	t1, t2 := filterAndSplitter.splitFunction(transfer)
	//  filterAndSplitter.outputExchange.Send(t1)
	//  filterAndSplitter.outputExchange.Send(t2)
	// }
}

func (filterAndSplitter *FilterAndSplitter) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
