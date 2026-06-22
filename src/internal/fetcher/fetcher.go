package fetcher

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/priorityqueue"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	API            = "https://api.frankfurter.dev/v2"
	ENDPOINT       = "/rate/%s/%s"
	QUERY          = "?date=%s"
	FULL_ENDPOINT  = API + ENDPOINT + QUERY
	DATE_LAYOUT    = "2006-01-02"
	CACHE_MAX_SIZE = 100_000
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

func createFetcherImpl(config FetcherConfig) (worker.Worker, error) {
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

	outputQueue, err := middleware.CreateQueueMiddleware(
		config.OutputQueue,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	return &Fetcher{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		queryId:     config.QueryID,
		quote:       config.Quote,
		ratesCache:  make(map[string]float64),
		ratesCacheHeap: priorityqueue.NewHeap(func(a, b heapDTO) int {
			if a.time.Before(b.time) {
				return 1
			} else if a.time.After(b.time) {
				return -1
			} else {
				return 0
			}
		}),
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

	input, err := batch.Read(msg.Body, records.TransferForQ5FilterCodec)
	if err != nil {
		slog.Error("while deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		eofBody := batch.WriteEOF(input.ClientID, fetcher.queryId, 0, 0, fetcher.forwarded)

		if err := fetcher.outputQueue.Send(middleware.Message{Body: eofBody}); err != nil {
			slog.Error("while sending EOF to filter amount", "err", err)
		}

		return
	}

	var responses []fetcherresponse.FetcherResponse
	for i := range input.Records {
		t := input.Records[i]
		if t.Currency == "Bitcoin" {
			continue
		}
		fetcher.forwarded++
		base := datasetToFrank[t.Currency]
		if base == fetcher.quote {
			responses = append(responses, fetcherresponse.FetcherResponse{ConvertedAmount: t.AmountPaid})
			continue
		}
		dateKey := t.Timestamp.Format(DATE_LAYOUT)
		cacheKey := dateKey + ":" + base
		rate, ok := fetcher.ratesCache[cacheKey]
		if !ok {
			var err error
			rate, err = fetcher.fetchExchangeRate(t)
			if err != nil {
				slog.Error("while fetching exchange rate", "err", err)
				continue
			}
			if len(fetcher.ratesCache) >= CACHE_MAX_SIZE {
				if !fetcher.ratesCacheHeap.IsEmpty() {
					oldest := fetcher.ratesCacheHeap.Dequeue()
					delete(fetcher.ratesCache, oldest.rate.Date+":"+oldest.rate.Base)
				}
			}
			fetcher.ratesCache[cacheKey] = rate
			fetcher.ratesCacheHeap.Enqueue(heapDTO{
				time: time.Now(),
				rate: &apiResponseRate{
					Date:  dateKey,
					Base:  base,
					Rate:  rate,
					Quote: fetcher.quote,
				},
			})
		}
		responses = append(responses, fetcherresponse.FetcherResponse{ConvertedAmount: t.AmountPaid * rate})
	}

	if len(responses) == 0 {
		return
	}
	body := batch.Write(input.ClientID, fetcher.queryId, 0, 0, responses, records.FetcherResponseCodec)
	if err := fetcher.outputQueue.Send(middleware.Message{Body: body}); err != nil {
		slog.Error("while publishing batch to output queue", "err", err)
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
		return 0, fmt.Errorf("received non-OK response: %s", response.Status)
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

	if err := fetcher.outputQueue.Close(); err != nil {
		slog.Error("while closing output queue", "err", err)
	}
}
