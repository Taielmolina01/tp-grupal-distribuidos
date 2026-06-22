package aggregate

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/summethod"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

const FILE_NAME = "avg_aggregator_%d"

func NewAvgAggregator(config AggregateConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	oldConnSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware newmiddleware.Middleware
		outputQueues    []middleware.Middleware
		err             error
	)

	defer func() {
		if err == nil {
			return
		}
		for _, q := range outputQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing output queue", "id", config.Id, "err", err)
			}
		}
		if inputMiddleware != nil {
			if err := inputMiddleware.Close(); err != nil {
				slog.Error("While closing input middleware", "id", config.Id, "err", err)
			}
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputQueues = make([]middleware.Middleware, 0, len(config.OutputQueues))
	for _, q := range config.OutputQueues {
		m, e := middleware.CreateQueueMiddleware(q, oldConnSettings)
		if e != nil {
			err = fmt.Errorf("creating output queue %s: %w", q, e)
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	msgMonitor := msgmonitor.NewShardedMessageMonitor()

	a := &AvgAggregator{
		id:              config.Id,
		queryID:         config.QueryID,
		inputMiddleware: inputMiddleware,
		outputQueues:    outputQueues,
		msgMonitor:      msgMonitor,
		acumuladores:    map[int]map[string]partial{},
		eofsByClient:    map[int]eofInfo{},
		expectedEOFs:    config.ExpectedEOFs,
	}

	if err := a.msgMonitor.LoadFromDisk(fmt.Sprintf(FILE_NAME, a.id)); err != nil {
		slog.Error("While loading message monitor from disk", "err", err)
	}

	return a, nil
}

func (a *AvgAggregator) Run() {
	defer a.close()
	slog.Info("Starting avg-aggregator consumers", "aggregate_id", a.id)

	if err := a.inputMiddleware.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		a.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (a *AvgAggregator) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	if err := a.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input middleware consumer", "aggregate_id", a.id, "err", err)
	}
}

func (a *AvgAggregator) close() {
	if err := a.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "aggregate_id", a.id, "err", err)
	}
	for _, q := range a.outputQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing output queue", "aggregate_id", a.id, "err", err)
		}
	}
}

func (a *AvgAggregator) handleInput(msg newmiddleware.Message, ack func()) {
	defer ack()

	input, err := summethod.Read(msg.Body)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		a.handleEOF(input.ClientID, input.Total)
		return
	}

	for _, record := range input.Records {
		a.handleRecord(input.ClientID, record)
	}

	if err := a.msgMonitor.SaveToDisk(fmt.Sprintf(FILE_NAME, a.id)); err != nil {
		slog.Error("While saving message monitor to disk", "err", err)
	}
}

func (a *AvgAggregator) handleRecord(clientID int, p transfer.SumByMethod) {
	method := p.Method

	a.mu.Lock()
	if a.acumuladores[clientID] == nil {
		a.acumuladores[clientID] = map[string]partial{}
	}
	existing, ok := a.acumuladores[clientID][method]
	if !ok {
		a.acumuladores[clientID][method] = partial{
			totalSum:   p.Sum,
			totalCount: p.Amount,
		}
		a.msgMonitor.AddForwardedMessagesAmountByClientId(clientID, 1)
	} else {
		a.acumuladores[clientID][method] = partial{
			totalSum:   existing.totalSum + p.Sum,
			totalCount: existing.totalCount + p.Amount,
		}
	}
	a.mu.Unlock()
	a.msgMonitor.AddProcessedMessagesAmountByClientId(clientID, 1)
}

func (a *AvgAggregator) handleEOF(clientID int, total uint32) {
	info, ok := a.eofsByClient[clientID]
	if !ok {
		a.eofsByClient[clientID] = eofInfo{
			amount:    1,
			processed: total,
		}
	} else {
		info.amount++
		info.processed += total
		a.eofsByClient[clientID] = info
	}

	if info.amount < a.expectedEOFs {
		return
	}

	if info.processed != a.msgMonitor.GetProcessedMessagesAmountByClientId(clientID) {
		slog.Warn("Received EOF with total that does not match processed messages amount",
			"client_id", clientID,
			"total_in_eof", info.processed,
			"processed_amount", a.msgMonitor.GetProcessedMessagesAmountByClientId(clientID),
		)
	}

	a.mu.Lock()
	byMethod := a.acumuladores[clientID]
	delete(a.acumuladores, clientID)
	a.mu.Unlock()

	avgs := make([]transfer.AvgByMethod, 0, len(byMethod))
	for method, p := range byMethod {
		if p.totalCount == 0 {
			continue
		}
		avgs = append(avgs, transfer.AvgByMethod{
			Method: method,
			Avg:    p.totalSum / float64(p.totalCount),
		})
	}

	if len(avgs) > 0 {
		body := avgmethod.WriteBatch(clientID, a.queryID, 0, 0, avgs)
		for _, q := range a.outputQueues {
			if err := q.Send(middleware.Message{Body: body}); err != nil {
				slog.Error("While sending output message", "client_id", clientID, "err", err)
			}
		}
	}

	eofBody := avgmethod.WriteEOF(clientID, a.queryID, 0, 0, uint32(len(avgs)))
	for _, q := range a.outputQueues {
		if err := q.Send(middleware.Message{Body: eofBody}); err != nil {
			slog.Error("While sending output EOF", "client_id", clientID, "err", err)
		}
	}
}
