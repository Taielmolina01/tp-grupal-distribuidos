package filter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	DATE_LAYOUT           = "2006-01-02"
	DATE_LAYOUT_WITH_HOUR = "2006-01-02 15:04"
	FILE_LAYOUT           = "%s_%s.csv"
	eofRingQueuePrefix    = "FILTER_CONVERTED_AMOUNT"
)

func newConvertedAmountFilter(
	config FilterConfig,
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueue,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(
		config.OutputQueue,
		connSettings,
	)

	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("while closing input queue", "err", err)
		}
		return nil, err
	}

	return &ConvertedAmountFilter{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		queryId:         config.QueryId,
		id:              uint32(config.Id),
		quote:           config.Quote,
		amountThreshold: config.Amount,
	}, nil
}

func (filter *ConvertedAmountFilter) Run() {
	defer filter.close()

	if err := filter.inputQueue.StartConsuming(filter.consume); err != nil {
		slog.Error("while starting consuming from input queue", "err", err)
		return
	}
}

func (filter *ConvertedAmountFilter) consume(msg middleware.Message, ack, _ func()) {
	defer ack()

	result, err := inner.DeserializeData[fetcherresponse.FetcherResponse](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}
	if result.IsEOF() {
		msgToOutput, err := inner.SerializeData(inner.DataMsg[transfer.FinalTransferForQ5]{
			ClientID: result.ClientID,
			EOF:      &inner.EOFInfo{},
			Payload:  transfer.ProjectForQ5Final(),
			QueryID:  result.QueryID,
		})
		if err != nil {
			slog.Error("while serializing message for finish callback", "client_id", result.ClientID, "err", err)
		} else {
			if err := filter.outputQueue.Send(*msgToOutput); err != nil {
				slog.Error("while calling finish callback", "client_id", result.ClientID, "err", err)
				return
			}
		}
		return
	}

	if result.Payload.ConvertedAmount < filter.amountThreshold {
		msgToOutput, err := inner.SerializeData(inner.DataMsg[transfer.FinalTransferForQ5]{
			ClientID: result.ClientID,
			EOF:      nil,
			Payload:  transfer.FinalTransferForQ5{},
			QueryID:  result.QueryID,
		})
		if err != nil {
			slog.Error("while serializing message for output", "client_id", result.ClientID, "err", err)
		} else {
			if err := filter.outputQueue.Send(*msgToOutput); err != nil {
				slog.Error("while sending message to output queue", "client_id", result.ClientID, "err", err)
				return
			}
		}
	}
}

func (filter *ConvertedAmountFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := filter.inputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from left input queue", "err", err)
	}
}

func (filter *ConvertedAmountFilter) close() {

	if err := filter.inputQueue.Close(); err != nil {
		slog.Error("while closing input queue", "err", err)
	}
	if err := filter.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
