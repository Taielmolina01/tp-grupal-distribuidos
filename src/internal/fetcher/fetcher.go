package fetcher

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/transfer"
)

// https://frankfurter.dev/
const (
	API           = "https://api.frankfurter.dev/v2"
	ENDPOINT      = "/rates"
	QUERY         = "?base=%s&date=%s"
	FULL_ENDPOINT = API + ENDPOINT + QUERY
	DATE_LAYOUT   = "2006-01-02"
)

func createFetcherImpl(config FetcherConfig) (*Fetcher, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, len(config.OutputQueues))
	for i, queueName := range config.OutputQueues {
		outputQueue, err := middleware.CreateQueueMiddleware(
			queueName,
			connSettings,
		)

		if err != nil {
			return nil, err
		}

		outputQueues[i] = outputQueue
	}

	return &Fetcher{
		inputQueue:       inputQueue,
		outputQueues:     outputQueues,
		queryId:          config.QueryId,
		quote:            config.Quote,
		conversionsByDay: make(map[string]map[string]float64),
	}, nil
}

func (fetcher *Fetcher) Run() {
	defer fetcher.close()

	if err := fetcher.inputQueue.StartConsuming(fetcher.consume); err != nil {
		slog.Error("while starting consuming from input queue", "err", err)
		return
	}
}

func (fetcher *Fetcher) consume(msg middleware.Message, ack, nack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.TransferForQ5FilterCodec)
	if err != nil {
		slog.Error("while deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		eofBody := batch.WriteEOF(input.ClientID, fetcher.queryId, input.Total)
		for _, outputQueue := range fetcher.outputQueues {
			if err := outputQueue.Send(middleware.Message{Body: string(eofBody)}); err != nil {
				slog.Error("while sending EOF to filter amount", "err", err)
			}
		}
		return
	}

	var responses []fetcherresponse.FetcherResponse
	for i := range input.Records {
		t := input.Records[i]
		today := t.Timestamp.Format(DATE_LAYOUT)
		if _, ok := fetcher.conversionsByDay[today]; ok {
			continue
		}
		fetcher.conversionsByDay[today] = make(map[string]float64)
		if err := fetcher.fetchExchangeRate(t); err != nil {
			slog.Error("while fetching exchange rate", "err", err)
			continue
		}
		for base, rate := range fetcher.conversionsByDay[today] {
			responses = append(responses, fetcherresponse.FetcherResponse{Date: today, Quote: base, Rate: rate})
		}
	}

	if len(responses) == 0 {
		return
	}
	body := batch.Write(input.ClientID, fetcher.queryId, responses, records.FetcherResponseCodec)
	for _, outputQueue := range fetcher.outputQueues {
		if err := outputQueue.Send(middleware.Message{Body: string(body)}); err != nil {
			slog.Error("while publishing batch to output queue", "err", err)
		}
	}
}

func (fetcher *Fetcher) fetchExchangeRate(transfer transfer.TransferForQ5Filter) error {
	response, err := http.Get(fmt.Sprintf(FULL_ENDPOINT, fetcher.quote, transfer.Timestamp.Format("2006-01-02")))
	if err != nil {
		return err
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("while closing response body", "err", err)
		}
	}()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-OK response: %s", response.Status)
	}

	decoder := json.NewDecoder(response.Body)
	var body []apiResponseRates
	if err = decoder.Decode(&body); err != nil {
		return fmt.Errorf("error decoding response body: %v", err)
	}

	for _, row := range body {
		if _, ok := fetcher.conversionsByDay[row.Date]; !ok {
			fetcher.conversionsByDay[row.Date] = make(map[string]float64)
		}
		fetcher.conversionsByDay[row.Date][row.Quote] = row.Rate
	}

	return nil
}

func (fetcher *Fetcher) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := fetcher.inputQueue.StopConsuming(); err != nil {
		slog.Error("while stopping consuming from input queue", "err", err)
	}
}

func (fetcher *Fetcher) close() {
	if err := fetcher.inputQueue.Close(); err != nil {
		slog.Error("while closing input queue", "err", err)
	}

	for _, outputQueue := range fetcher.outputQueues {
		if err := outputQueue.Close(); err != nil {
			slog.Error("while closing output queue", "err", err)
		}
	}
}
