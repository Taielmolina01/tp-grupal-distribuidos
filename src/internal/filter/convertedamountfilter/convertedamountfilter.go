package convertedamountfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	DATE_LAYOUT           = "2006-01-02"
	DATE_LAYOUT_WITH_HOUR = "2006-01-02 15:04"
	FILE_LAYOUT           = "%s_%s.csv"
	eofRingQueuePrefix    = "FILTER_CONVERTED_AMOUNT"
)

func CreateConvertedAmountFilter(config filter.FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
	)
}

func newConvertedAmountFilter(
	config filter.FilterConfig,
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

	messagesMonitor := msgmonitor.NewMessageMonitor()

	filter := &ConvertedAmountFilter{
		InputQueue:      inputQueue,
		OutputQueue:     outputQueue,
		QueryId:         config.QueryId,
		HandlerMessages: messagesMonitor,
		Id:              uint32(config.Id),
		Quote:           config.Quote,
		AmountThreshold: config.Amount,
		Pending:         make(map[int][]transfer.FinalTransferForQ5),
	}

	return filter, nil
}

func (filter *ConvertedAmountFilter) Run() {
	defer filter.close()

	if err := filter.InputQueue.StartConsuming(filter.consume); err != nil {
		slog.Error("while starting consuming from left input queue", "err", err)
		return
	}
}

func (filter *ConvertedAmountFilter) consume(msg middleware.Message, ack, _ func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.FetcherResponseCodec)
	if err != nil {
		slog.Error("while deserializing batch", "err", err)
		return
	}
	if input.EOF {
		filter.flush(input.ClientID)
		if err := filter.OutputQueue.Send(middleware.Message{Body: string(batch.WriteEOF(input.ClientID, filter.QueryId, input.Total))}); err != nil {
			slog.Error("while forwarding EOF", "client_id", input.ClientID, "err", err)
		}
		return
	}

	for _, t := range input.Records {
		if t.ConvertedAmount < filter.AmountThreshold {
			filter.emit(input.ClientID, transfer.ProjectForQ5Final())
		}
	}
}

// Helpers

func (filter *ConvertedAmountFilter) emit(clientID int, response transfer.FinalTransferForQ5) {
	filter.Pending[clientID] = append(filter.Pending[clientID], response)
}

func (filter *ConvertedAmountFilter) flush(clientID int) {
	results := filter.Pending[clientID]
	delete(filter.Pending, clientID)

	if len(results) == 0 {
		return
	}
	body := batch.Write(clientID, filter.QueryId, results, records.FinalTransferForQ5Codec)
	if err := filter.OutputQueue.Send(middleware.Message{Body: string(body)}); err != nil {
		slog.Error("while sending results batch", "err", err)
	}
}

// Closure

func (filter *ConvertedAmountFilter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := filter.InputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from left input queue", "err", err)
	}
}

func (filter *ConvertedAmountFilter) close() {
	if err := filter.InputQueue.Close(); err != nil {
		slog.Error("while closing left input queue", "err", err)
	}
	if err := filter.OutputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
