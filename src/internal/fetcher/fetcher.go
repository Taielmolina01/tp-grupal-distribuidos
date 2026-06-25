package fetcher

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/priorityqueue"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const (
	_API              = "https://api.frankfurter.dev/v2"
	_ENDPOINT         = "/rate/%s/%s"
	_QUERY            = "?date=%s"
	_FULL_ENDPOINT    = _API + _ENDPOINT + _QUERY
	_DATE_LAYOUT      = "2006-01-02"
	_CACHE_MAX_SIZE   = 100_000
	_IGNORED_CURRENCY = "Bitcoin"
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
	connSettings := newmiddleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue := fmt.Sprintf("%s_%d", config.InputMiddlewarePrefix, config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err := newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)

	if err != nil {
		return nil, err
	}

	outputMiddleware, err := newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	checkpoint, err := checkpoint.New(
		config.PersistPath,
		MarshalClientState,
		UnmarshalClientState,
	)

	if err != nil {
		return nil, err
	}

	recovered, err := checkpoint.Load()
	if err != nil {
		return nil, err
	}

	states := statemap.New(func() *clientState {
		return &clientState{tracker: sendertracker.New(10_000_000), outputTracker: outputtracker.New()}
	})

	for clientID, state := range recovered {
		states.Set(clientID, state)
	}

	return &Fetcher{
		inputQueue:       inputMiddleware,
		outputMiddleware: outputMiddleware,
		queryId:          config.QueryID,
		quote:            config.Quote,
		ratesCache:       make(map[string]heapDTO),
		ratesCacheHeap: priorityqueue.NewHeap(func(a, b heapDTO) int {
			if a.time.Before(b.time) {
				return 1
			} else if a.time.After(b.time) {
				return -1
			} else {
				return 0
			}
		}),
		states:          states,
		checkpoint:      checkpoint,
		expectedSenders: config.ExpectedInputSenders,
		outputAmount:    config.OutputAmount,
		hasher:          shard.New(config.OutputAmount),
	}, nil
}

func (fetcher *Fetcher) Run() {
	defer fetcher.close()

	if err := fetcher.inputQueue.StartConsuming(fetcher.consume); err != nil {
		slog.Error("while starting consuming from input queue", "err", err)
		return
	}
}

func (fetcher *Fetcher) consume(msg newmiddleware.Message, ack, nack func()) {
	input, err := batch.Read(msg.Body, records.TransferForQ5FilterCodec)
	if err != nil {
		slog.Error("while deserializing input batch", "err", err)
		ack()
		return
	}

	clientID := input.ClientID
	state := fetcher.states.For(clientID)
	tracker := state.tracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
		ack()
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		tracker.Claim(int(input.SenderID), input.Seq)
		if !tracker.IsComplete(fetcher.expectedSenders) {
			if err := fetcher.checkpoint.SaveClient(clientID, state); err != nil {
				slog.Error("persist failed, stopping", "err", err)
				nack()
				if err := fetcher.inputQueue.StopConsuming(); err != nil {
					slog.Error("while stopping consuming from input queue", "err", err)
				}
				return
			}
			ack()
			return
		}
		fetcher.finishTransfersStep(clientID, state)
		ack()
		return
	}

	tracker.RegisterBatch(int(input.SenderID))
	tracker.Claim(int(input.SenderID), input.Seq)

	byRoutingKeys := make(map[string][]fetcherresponse.FetcherResponse)

	for i := range input.Records {
		t := input.Records[i]
		if t.Currency == _IGNORED_CURRENCY {
			continue
		}
		base := datasetToFrank[t.Currency]
		var response fetcherresponse.FetcherResponse
		if base == fetcher.quote {
			response = fetcherresponse.FetcherResponse{ConvertedAmount: t.AmountPaid}
		} else {
			fetcher.fetchExchangeRateWithCache(&response, t, base)
		}
		keys := []string{strconv.Itoa(int(input.Seq))}
		rk := fmt.Sprintf("shard-%d", fetcher.hasher.ShardFor(input.ClientID, keys...))
		byRoutingKeys[rk] = append(byRoutingKeys[rk], response)
	}

	if len(byRoutingKeys) == 0 {
		ack()
		return
	}

	for rk, group := range byRoutingKeys {
		body := batch.Write(input.ClientID, fetcher.queryId, 0, input.Seq, group, records.FetcherResponseCodec)
		if err := fetcher.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			slog.Error("while publishing batch to output cluster", "err", err)
		}
		state.outputTracker.RegisterBatch(rk)
	}

	if tracker.IsComplete(fetcher.expectedSenders) {
		if err := fetcher.finishTransfersStep(clientID, state); err != nil {
			slog.Error("while finishing transfers step", "err", err)
			nack()
			if err := fetcher.inputQueue.StopConsuming(); err != nil {
				slog.Error("while stopping consuming from input queue", "err", err)
			}
			return
		}
		fetcher.states.Delete(clientID)
		fetcher.checkpoint.DeleteClient(clientID)
	}

	if err := fetcher.checkpoint.SaveClient(clientID, state); err != nil {
		slog.Error("persist failed, stopping", "err", err)
		nack()
		if err := fetcher.inputQueue.StopConsuming(); err != nil {
			slog.Error("while stopping consuming from input queue", "err", err)
		}
		return
	}
	ack()
}

func (fetcher *Fetcher) finishTransfersStep(clientID int, state *clientState) error {
	for i := range fetcher.outputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		count := uint32(state.outputTracker.CountFor(rk))
		eofBody := batch.WriteEOF(clientID, fetcher.queryId, 0, 0, count)
		if err := fetcher.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			return err
		}
	}
	fetcher.states.Delete(clientID)
	fetcher.checkpoint.DeleteClient(clientID)
	return nil
}

func (fetcher *Fetcher) fetchExchangeRateWithCache(
	response *fetcherresponse.FetcherResponse,
	t transfer.TransferForQ5Filter,
	base string,
) {
	dateKey := t.Timestamp.Format(_DATE_LAYOUT)
	cacheKey := dateKey + ":" + base
	oldValueCache, ok := fetcher.ratesCache[cacheKey]
	if !ok {
		var err error
		fetchedRate, err := fetcher.fetchExchangeRate(t)
		if err != nil {
			slog.Error("while fetching exchange rate", "err", err)
			return
		}
		if len(fetcher.ratesCache) >= _CACHE_MAX_SIZE && !fetcher.ratesCacheHeap.IsEmpty() {
			oldest := fetcher.ratesCacheHeap.Dequeue()
			delete(fetcher.ratesCache, oldest.apiResponseRateVal.Date+":"+oldest.apiResponseRateVal.Base)
		}
		dto := heapDTO{
			time: time.Now(),
			apiResponseRateVal: &apiResponseRate{
				Date:  dateKey,
				Base:  base,
				Rate:  fetchedRate,
				Quote: fetcher.quote,
			},
		}
		fetcher.ratesCacheHeap.Enqueue(dto)
		fetcher.ratesCache[cacheKey] = dto
		response.ConvertedAmount = t.AmountPaid * fetchedRate
	} else {
		new := heapDTO{
			time: time.Now(),
			apiResponseRateVal: &apiResponseRate{
				Date:  dateKey,
				Base:  base,
				Rate:  oldValueCache.apiResponseRateVal.Rate,
				Quote: fetcher.quote,
			},
		}
		fetcher.ratesCacheHeap.Update(
			oldValueCache,
			new,
		)
		fetcher.ratesCache[cacheKey] = new
		response.ConvertedAmount = t.AmountPaid * oldValueCache.apiResponseRateVal.Rate
	}
}

func (fetcher *Fetcher) fetchExchangeRate(transfer transfer.TransferForQ5Filter) (float64, error) {
	response, err := http.Get(fmt.Sprintf(_FULL_ENDPOINT, datasetToFrank[transfer.Currency], fetcher.quote, transfer.Timestamp.Format(_DATE_LAYOUT)))
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
	cleanup.Close(fetcher.inputQueue, fetcher.outputMiddleware)
}
