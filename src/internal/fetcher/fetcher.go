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
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const (
	API           = "https://api.frankfurter.dev/v2"
	ENDPOINT      = "/rate/%s/%s"
	QUERY         = "?date=%s"
	FULL_ENDPOINT = API + ENDPOINT + QUERY
	DATE_LAYOUT   = "2006-01-02"
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
		inputQueue:   inputQueue,
		outputQueues: outputQueues,
		queryId:      config.QueryId,
		quote:        config.Quote,
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

	result, err := inner.DeserializeData[transfer.TransferForQ5Filter](&msg)
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

	if transfer.Currency == "Bitcoin" {
		return
	}

	if datasetToFrank[transfer.Currency] == fetcher.quote {
		msgOutput, err := inner.SerializeData(inner.DataMsg[fetcherresponse.FetcherResponse]{
			ClientID: result.ClientID,
			QueryID:  fetcher.queryId,
			Payload: fetcherresponse.FetcherResponse{
				ConvertedAmount: transfer.AmountPaid,
			},
			EOF: nil,
		})
		if err != nil {
			slog.Error("while serializing message", "err", err)
			return
		}
		if err := fetcher.outputQueues[shard.CalculateIndexForShard(result.ClientID, transfer.Currency, len(fetcher.outputQueues))].Send(*msgOutput); err != nil {
			slog.Error("while publishing message to output queue", "err", err)
		}
		return
	}
	rate, err := fetcher.fetchExchangeRate(transfer)
	if err != nil {
		slog.Error("while fetching exchange rate", "err", err)
		return
	}
	msgOutput, err := inner.SerializeData(inner.DataMsg[fetcherresponse.FetcherResponse]{
		ClientID: result.ClientID,
		QueryID:  fetcher.queryId,
		Payload: fetcherresponse.FetcherResponse{
			ConvertedAmount: transfer.AmountPaid * rate,
		},
		EOF: nil,
	})
	if err != nil {
		slog.Error("while serializing message", "err", err)
		return
	}
	if err := fetcher.outputQueues[shard.CalculateIndexForShard(result.ClientID, transfer.Currency, len(fetcher.outputQueues))].Send(*msgOutput); err != nil {
		slog.Error("while publishing message to output queue", "err", err)
	}
}

func (fetcher *Fetcher) fetchExchangeRate(transfer transfer.TransferForQ5Filter) (float64, error) {
	response, err := http.Get(fmt.Sprintf(FULL_ENDPOINT, datasetToFrank[transfer.Currency], fetcher.quote, transfer.Timestamp.Format("2006-01-02")))
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("while closing response body", "err", err)
		}
	}()

	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("received non-OK response: %s. \nresponse: %s\nendp: %s", response.Status, response.Body, fmt.Sprintf(FULL_ENDPOINT, datasetToFrank[transfer.Currency], fetcher.quote, transfer.Timestamp.Format("2006-01-02")))
	}

	decoder := json.NewDecoder(response.Body)
	var body apiResponseRate
	if err = decoder.Decode(&body); err != nil {
		return 0, fmt.Errorf("error decoding response body: %v", err)
	}

	return body.Rate, nil
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
