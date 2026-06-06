package aggregate

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/summethod"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const eofRingPrefix = "AGGREGATE_"

type AggregateConfig struct {
	Id              int
	AggregateAmount int
	MomHost         string
	MomPort         int
	InputQueue      string
	OutputQueues    []string
	QueryID         uint8
}

type partial struct {
	totalSum   float64
	totalCount int
}

type AvgAggregator struct {
	id      int
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	eofInput     middleware.Middleware
	eofOutput    middleware.Middleware
	eofHandler   eofring.EofRingAlgorithm
	msgMonitor   msgmonitor.MessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]partial
}

func getRingNextIndex(config AggregateConfig) int {
	if config.Id == config.AggregateAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewAvgAggregator(config AggregateConfig) (_ *AvgAggregator, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputQueue   middleware.Middleware
		outputQueues []middleware.Middleware
		eofInput     middleware.Middleware
		eofOutput    middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		if eofOutput != nil {
			if err := eofOutput.Close(); err != nil {
				slog.Error("While closing EOF output", "id", config.Id, "err", err)
			}
		}
		if eofInput != nil {
			if err := eofInput.Close(); err != nil {
				slog.Error("While closing EOF input", "id", config.Id, "err", err)
			}
		}
		for _, q := range outputQueues {
			if err := q.Close(); err != nil {
				slog.Error("While closing output queue", "id", config.Id, "err", err)
			}
		}
		if inputQueue != nil {
			if err := inputQueue.Close(); err != nil {
				slog.Error("While closing input queue", "id", config.Id, "err", err)
			}
		}
	}()

	inputQueue, err = middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, fmt.Errorf("creating input queue: %w", err)
	}

	outputQueues = make([]middleware.Middleware, 0, len(config.OutputQueues))
	for _, q := range config.OutputQueues {
		m, e := middleware.CreateQueueMiddleware(q, connSettings)
		if e != nil {
			err = fmt.Errorf("creating output queue %s: %w", q, e)
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	eofInput, err = middleware.CreateQueueMiddleware(
		eofRingPrefix+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating EOF input queue: %w", err)
	}

	eofOutput, err = middleware.CreateQueueMiddleware(
		eofRingPrefix+strconv.Itoa(getRingNextIndex(config)),
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating EOF output queue: %w", err)
	}

	msgMonitor := msgmonitor.NewMessageMonitor()

	a := &AvgAggregator{
		id:           config.Id,
		queryID:      config.QueryID,
		inputQueue:   inputQueue,
		outputQueues: outputQueues,
		eofInput:     eofInput,
		eofOutput:    eofOutput,
		msgMonitor:   msgMonitor,
		acumuladores: map[int]map[string]partial{},
	}

	a.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.AggregateAmount,
		uint32(config.Id),
		msgMonitor,
		a.onRingConverged,
		config.QueryID,
	)

	return a, nil
}

func (a *AvgAggregator) Run() {
	defer a.close()
	slog.Info("Starting avg-aggregator consumers", "aggregate_id", a.id)
	go a.eofHandler.Run()
	if err := a.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		a.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (a *AvgAggregator) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	if err := a.inputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping input queue consumer", "aggregate_id", a.id, "err", err)
	}
	if err := a.eofInput.StopConsuming(); err != nil {
		slog.Error("While stopping EOF input consumer", "aggregate_id", a.id, "err", err)
	}
}

func (a *AvgAggregator) close() {
	if err := a.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "aggregate_id", a.id, "err", err)
	}
	if err := a.eofInput.Close(); err != nil {
		slog.Error("While closing EOF input", "aggregate_id", a.id, "err", err)
	}
	if err := a.eofOutput.Close(); err != nil {
		slog.Error("While closing EOF output", "aggregate_id", a.id, "err", err)
	}
	for _, q := range a.outputQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing output queue", "aggregate_id", a.id, "err", err)
		}
	}
}

func (a *AvgAggregator) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	input, err := summethod.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		a.handleEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		a.handleRecord(input.ClientID, input.Records[i])
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
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   a.msgMonitor.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(a.id),
		FilteredAmount: a.msgMonitor.GetForwardedMessagesAmountByClientId(clientID),
	}
	if err := a.eofOutput.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(ringMsg))}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
		return
	}
}

func (a *AvgAggregator) onRingConverged(clientID int, total uint32, isCoordinator bool) error {
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
		body := avgmethod.WriteBatch(clientID, a.queryID, avgs)
		for _, q := range a.outputQueues {
			if err := q.Send(middleware.Message{Body: string(body)}); err != nil {
				return err
			}
		}
	}

	if !isCoordinator {
		return nil
	}
	eofBody := avgmethod.WriteEOF(clientID, a.queryID, total)
	for _, q := range a.outputQueues {
		if err := q.Send(middleware.Message{Body: string(eofBody)}); err != nil {
			return err
		}
	}
	return nil
}
