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
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
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
			eofOutput.Close()
		}
		if eofInput != nil {
			eofInput.Close()
		}
		for _, q := range outputQueues {
			q.Close()
		}
		if inputQueue != nil {
			inputQueue.Close()
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

	a.inputQueue.StopConsuming()
	a.eofInput.StopConsuming()
}

func (a *AvgAggregator) close() {
	a.inputQueue.Close()
	a.eofInput.Close()
	a.eofOutput.Close()
	for _, q := range a.outputQueues {
		q.Close()
	}
}

func (a *AvgAggregator) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.SumByMethod](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		a.handleEOF(*result)
		return
	}

	a.handleRecord(result.ClientID, result.Payload)
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

func (a *AvgAggregator) handleEOF(data inner.DataMsg[transfer.SumByMethod]) {
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     data.EOF.TotalMessages,
		ActualAmount:   a.msgMonitor.GetProcessedMessagesAmountByClientId(data.ClientID),
		ClientId:       data.ClientID,
		CoordinatorId:  uint32(a.id),
		FilteredAmount: a.msgMonitor.GetForwardedMessagesAmountByClientId(data.ClientID),
	}
	serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
	if err != nil {
		slog.Error("While serializing EOF ring message", "err", err)
		return
	}
	if err := a.eofOutput.Send(*serialized); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
		return
	}
}

func (a *AvgAggregator) onRingConverged(clientID int, msg *middleware.Message, isCoordinator bool) error {
	a.mu.Lock()
	byMethod := a.acumuladores[clientID]
	delete(a.acumuladores, clientID)
	a.mu.Unlock()

	for method, p := range byMethod {
		if p.totalCount == 0 {
			continue
		}
		avg := transfer.AvgByMethod{
			Method: method,
			Avg:    p.totalSum / float64(p.totalCount),
		}
		out, err := inner.SerializeData(inner.DataMsg[transfer.AvgByMethod]{
			Payload:  avg,
			ClientID: clientID,
			QueryID:  a.queryID,
		})
		if err != nil {
			return err
		}
		for _, q := range a.outputQueues {
			if err := q.Send(*out); err != nil {
				return err
			}
		}
	}

	if !isCoordinator {
		return nil
	}
	for _, q := range a.outputQueues {
		if err := q.Send(*msg); err != nil {
			return err
		}
	}
	return nil
}
