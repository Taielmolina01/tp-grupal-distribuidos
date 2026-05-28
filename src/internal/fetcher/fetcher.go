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
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
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

	result, err := inner.DeserializeData[transfer.TransferForQ5](&msg)
	if err != nil {
		slog.Error("while deserializing transfer", "err", err)
		return
	}

	if result.EOF != nil {
		for _, outputQueue := range fetcher.outputQueues {
			if err := outputQueue.Send(msg); err != nil {
				slog.Error("while sending EOF to filter amount", "err", err)
			}
		}
		return
	}

	transfer := result.Payload

	today := transfer.Timestamp.Format(DATE_LAYOUT)
	if _, ok := fetcher.conversionsByDay[today]; !ok {
		fetcher.conversionsByDay[today] = make(map[string]float64)
		if err := fetcher.fetchExchangeRate(transfer); err != nil {
			slog.Error("while fetching exchange rate", "err", err)
			return
		}
		for base, rate := range fetcher.conversionsByDay[today] {
			for _, outputQueue := range fetcher.outputQueues {
				msgOutput, err := inner.SerializeData(inner.DataMsg[fetcherresponse.FetcherResponse]{
					ClientID: result.ClientID,
					QueryID:  fetcher.queryId,
					Payload: fetcherresponse.FetcherResponse{
						Date:  today,
						Quote: base,
						Rate:  rate,
					},
					EOF: nil,
				})
				if err != nil {
					slog.Error("while serializing message", "err", err)
					break
				}
				if err := outputQueue.Send(*msgOutput); err != nil {
					slog.Error("while publishing message to output queue", "err", err)
				}
			}
		}
	}

}

func (fetcher *Fetcher) fetchExchangeRate(transfer transfer.TransferForQ5) error {
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
