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
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
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
	newConnSettings := newmiddleware.ConnSettings{
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

	outputClusters := make([]outputCluster, 0, len(config.OutputClusters))
	for _, c := range config.OutputClusters {
		m, err := newmiddleware.NewShardedMiddleware(newConnSettings, c.Prefix, "", "")
		if err != nil {
			for _, cl := range outputClusters {
				if closeErr := cl.middleware.Close(); closeErr != nil {
					slog.Error("while closing output cluster middleware after creation failure", "err", closeErr)
				}
			}
			return nil, err
		}
		outputClusters = append(outputClusters, outputCluster{
			middleware: m,
			hasher:     shard.New(c.NodeCount),
		})
	}

	return &Fetcher{
		inputQueue:     inputQueue,
		outputClusters: outputClusters,
		queryId:        config.QueryID,
		quote:          config.Quote,
		ratesCache:     make(map[string]heapDTO),
		ratesCacheHeap: priorityqueue.NewHeap(func(a, b heapDTO) int {
			if a.time.Before(b.time) {
				return 1
			} else if a.time.After(b.time) {
				return -1
			} else {
				return 0
			}
		}),
		states: statemap.New(func() *clientState {
			return &clientState{tracker: sendertracker.New(10_000_000), outputTracker: outputtracker.New()}
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

	clientID := input.ClientID
	state := fetcher.states.For(clientID)
	tracker := state.tracker

	if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
		slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
		return
	}

	if input.EOF {
		tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		tracker.Claim(int(input.SenderID), input.Seq)
		if !tracker.IsComplete(fetcher.expectedSenders) {
			return
		}
		for ci, cluster := range fetcher.outputClusters {
			for i := range cluster.hasher.TotalShards() {
				rk := fmt.Sprintf("shard-%d", i)
				key := fmt.Sprintf("%d_%s", ci, rk)
				count := uint32(state.outputTracker.CountFor(key))
				eofBody := batch.WriteEOF(clientID, fetcher.queryId, 0, 0, count)
				if err := cluster.middleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
					slog.Error("while sending EOF to filter amount", "err", err)
				}
			}
		}
		return
	}

	tracker.RegisterBatch(int(input.SenderID))
	tracker.Claim(int(input.SenderID), input.Seq)

	type clusterKey struct {
		index int
		rk    string
	}
	byCluster := make(map[clusterKey][]fetcherresponse.FetcherResponse)

	for i := range input.Records {
		t := input.Records[i]
		if t.Currency == "Bitcoin" {
			continue
		}
		base := datasetToFrank[t.Currency]
		var response fetcherresponse.FetcherResponse
		if base == fetcher.quote {
			response = fetcherresponse.FetcherResponse{ConvertedAmount: t.AmountPaid}
		} else {
			fetcher.fetchExchangeRateWithCache(&response, t, base)
		}
		keys := []string{strconv.FormatFloat(t.AmountPaid, 'f', -1, 64)}
		for ci, cluster := range fetcher.outputClusters {
			rk := fmt.Sprintf("shard-%d", cluster.hasher.ShardFor(input.ClientID, keys...))
			ck := clusterKey{ci, rk}
			byCluster[ck] = append(byCluster[ck], response)
		}
	}

	if len(byCluster) == 0 {
		return
	}

	for ck, group := range byCluster {
		cluster := fetcher.outputClusters[ck.index]
		body := batch.Write(input.ClientID, fetcher.queryId, 0, input.Seq, group, records.FetcherResponseCodec)
		if err := cluster.middleware.Send(newmiddleware.Message{Body: body, RoutingKey: ck.rk}); err != nil {
			slog.Error("while publishing batch to output cluster", "err", err)
		}
		state.outputTracker.RegisterBatch(fmt.Sprintf("%d_%s", ck.index, ck.rk))
	}
}

func (fetcher *Fetcher) fetchExchangeRateWithCache(
	response *fetcherresponse.FetcherResponse,
	t transfer.TransferForQ5Filter,
	base string,
) {
	dateKey := t.Timestamp.Format(DATE_LAYOUT)
	cacheKey := dateKey + ":" + base
	oldValueCache, ok := fetcher.ratesCache[cacheKey]
	if !ok {
		var err error
		fetchedRate, err := fetcher.fetchExchangeRate(t)
		if err != nil {
			slog.Error("while fetching exchange rate", "err", err)
			return
		}
		if len(fetcher.ratesCache) >= CACHE_MAX_SIZE && !fetcher.ratesCacheHeap.IsEmpty() {
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
	cleanup.Close(fetcher.inputQueue)
	for _, cluster := range fetcher.outputClusters {
		cleanup.Close(cluster.middleware)
	}
}
