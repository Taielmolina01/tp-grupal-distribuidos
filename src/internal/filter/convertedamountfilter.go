package filter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
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

var datasetToFrank = map[string]string{
	"Australian Dollar": "AUD",
	"Bitcoin":           "BTC",
	"Brazil Real":       "BRL",
	"Canadian Dollar":   "CAD",
	"Euro":              "EUR",
	"Mexican Peso":      "MXN",
	"Ruble":             "RUB",
	"Rupee":             "INR",
	"Saudi Riyal":       "SAR",
	"Shekel":            "ILS",
	"Swiss Franc":       "CHF",
	"UK Pound":          "GBP",
	"US Dollar":         "USD",
	"Yen":               "JPY",
	"Yuan":              "CNY",
}

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

	messagesMonitor := msgmonitor.NewMessageMonitor()

	filter := &ConvertedAmountFilter{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		queryId:         config.QueryId,
		handlerMessages: messagesMonitor,
		id:              uint32(config.Id),
		quote:           config.Quote,
		amountThreshold: config.Amount,
		pending:         make(map[int][]transfer.FinalTransferForQ5),
	}

	return filter, nil
}

func (filter *ConvertedAmountFilter) Run() {
	defer filter.close()

	if err := filter.inputQueue.StartConsuming(filter.consume); err != nil {
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
		if err := filter.outputQueue.Send(middleware.Message{Body: string(batch.WriteEOF(input.ClientID, filter.queryId, input.Total))}); err != nil {
			slog.Error("while forwarding EOF", "client_id", input.ClientID, "err", err)
		}
		return
	}

	for _, t := range input.Records {
		if t.ConvertedAmount < filter.amountThreshold {
			filter.emit(input.ClientID, transfer.ProjectForQ5Final())
		}
	}
}

// Helpers

func (filter *ConvertedAmountFilter) emit(clientID int, response transfer.FinalTransferForQ5) {
	filter.pending[clientID] = append(filter.pending[clientID], response)
}

func (filter *ConvertedAmountFilter) flush(clientID int) {
	results := filter.pending[clientID]
	delete(filter.pending, clientID)

	if len(results) == 0 {
		return
	}
	body := batch.Write(clientID, filter.queryId, results, records.FinalTransferForQ5Codec)
	if err := filter.outputQueue.Send(middleware.Message{Body: string(body)}); err != nil {
		slog.Error("while sending results batch", "err", err)
	}
}

// Closure

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
		slog.Error("while closing left input queue", "err", err)
	}
	if err := filter.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
