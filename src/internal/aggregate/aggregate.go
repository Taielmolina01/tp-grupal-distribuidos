package aggregate

import (
	"log/slog"
	"strconv"
	"sync"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

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
	totalSum   float32
	totalCount int
}

type AvgAggregator struct {
	id              int
	inputQueue      newmiddleware.Middleware
	outputQueues    []newmiddleware.Middleware
	outputQueueEof  newmiddleware.Middleware
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	queryID         uint8

	acumuladores map[int]map[string]partial
	lock         sync.Mutex
}

func CreateAvgAggregator(config AggregateConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := newmiddleware.NewQueueMiddleware(connSettings, config.InputQueue)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]newmiddleware.Middleware, 0, len(config.OutputQueues))
	for _, q := range config.OutputQueues {
		m, err := newmiddleware.NewQueueMiddleware(connSettings, q)
		if err != nil {
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	next := config.Id + 1
	if config.Id == config.AggregateAmount-1 {
		next = 0
	}

	eofInput, err := newmiddleware.NewQueueMiddleware(
		connSettings,
		"AGGREGATE_"+strconv.Itoa(config.Id),
	)
	if err != nil {
		return nil, err
	}

	eofOutput, err := newmiddleware.NewQueueMiddleware(
		connSettings,
		"AGGREGATE_"+strconv.Itoa(next),
	)
	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	agg := &AvgAggregator{
		id:              config.Id,
		inputQueue:      inputQueue,
		outputQueues:    outputQueues,
		outputQueueEof:  eofOutput,
		handlerMessages: handlerMessages,
		queryID:         config.QueryID,
		acumuladores:    map[int]map[string]partial{},
	}

	agg.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.AggregateAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *newmiddleware.Message, isCoordinator bool) error {
			agg.lock.Lock()
			byMethod := agg.acumuladores[clientID]
			delete(agg.acumuladores, clientID)
			agg.lock.Unlock()

			for method, p := range byMethod {
				if p.totalCount == 0 {
					continue
				}
				avg := transfer.AvgByMethod{
					Method: method,
					Avg:    p.totalSum / float32(p.totalCount),
				}
				out, err := inner.SerializeData(inner.DataMsg[transfer.AvgByMethod]{
					Payload:  avg,
					ClientID: clientID,
					QueryID:  agg.queryID,
				})
				if err != nil {
					return err
				}
				slog.Info("Aggregate broadcasting avg", "client_id", clientID, "method", method, "avg", avg.Avg, "count", p.totalCount)
				for _, q := range agg.outputQueues {
					if err := q.Send(*out); err != nil {
						return err
					}
				}
			}

			if !isCoordinator {
				return nil
			}
			for _, q := range agg.outputQueues {
				if err := q.Send(*msg); err != nil {
					return err
				}
			}
			return nil
		},
		config.QueryID,
	)

	return agg, nil
}

func (a *AvgAggregator) Run() {
	slog.Info("Starting avg-aggregator consumers", "aggregate_id", a.id)
	go a.eofHandler.Run()
	if err := a.inputQueue.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		a.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (a *AvgAggregator) handleMessage(msg newmiddleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.SumByMethod](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		ringMsg := eofmessagetypes.EofRingMessage{
			RealAmount:     result.EOF.TotalMessages,
			ActualAmount:   a.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			CoordinatorId:  uint32(a.id),
			FilteredAmount: a.handlerMessages.GetForwardedMessagesAmountByClientId(result.ClientID),
		}
		serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
		if err != nil {
			slog.Error("While serializing EOF ring message", "err", err)
			return
		}
		if err := a.outputQueueEof.Send(*serialized); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		slog.Info("Aggregate fired ring", "aggregate_id", a.id, "client_id", result.ClientID, "real_amount", result.EOF.TotalMessages)
		return
	}

	p := result.Payload
	method := p.Method

	a.lock.Lock()
	if a.acumuladores[result.ClientID] == nil {
		a.acumuladores[result.ClientID] = map[string]partial{}
	}
	existing, ok := a.acumuladores[result.ClientID][method]
	if !ok {
		a.acumuladores[result.ClientID][method] = partial{
			totalSum:   p.Sum,
			totalCount: p.Amount,
		}
		a.handlerMessages.AddForwardedMessagesAmountByClientId(result.ClientID, 1)
	} else {
		a.acumuladores[result.ClientID][method] = partial{
			totalSum:   existing.totalSum + p.Sum,
			totalCount: existing.totalCount + p.Amount,
		}
	}
	a.lock.Unlock()
	a.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
}

func (a *AvgAggregator) HandleSignals() {
}
