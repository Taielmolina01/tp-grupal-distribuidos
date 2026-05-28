package sum

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
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const eofRingPrefix = "SUM_"

type SumConfig struct {
	Id           int
	SumAmount    int
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQueues []string
	QueryID      uint8
}

type SumByPaymentFormat struct {
	id      int
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	eofInput     middleware.Middleware
	eofOutput    middleware.Middleware
	eofHandler   eofring.EofRingAlgorithm
	msgMonitor   msgmonitor.MessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]transfer.SumByMethod
}

func getRingNextIndex(config SumConfig) int {
	if config.Id == config.SumAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewSumByPaymentFormat(config SumConfig) (_ *SumByPaymentFormat, err error) {
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

	s := &SumByPaymentFormat{
		id:           config.Id,
		queryID:      config.QueryID,
		inputQueue:   inputQueue,
		outputQueues: outputQueues,
		eofInput:     eofInput,
		eofOutput:    eofOutput,
		msgMonitor:   msgMonitor,
		acumuladores: map[int]map[string]transfer.SumByMethod{},
	}

	s.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.SumAmount,
		uint32(config.Id),
		msgMonitor,
		s.onRingConverged,
		config.QueryID,
	)

	return s, nil
}

func (s *SumByPaymentFormat) Run() {
	defer s.close()
	slog.Info("Starting sum-by-payment-format consumers", "sum_id", s.id)
	go s.eofHandler.Run()
	if err := s.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		s.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (s *SumByPaymentFormat) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	s.inputQueue.StopConsuming()
	s.eofInput.StopConsuming()
}

func (s *SumByPaymentFormat) close() {
	s.inputQueue.Close()
	s.eofInput.Close()
	s.eofOutput.Close()
	for _, q := range s.outputQueues {
		q.Close()
	}
}

func (s *SumByPaymentFormat) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.Transfer](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		s.handleEOF(*result)
		return
	}

	s.handleRecord(result.ClientID, result.Payload)
}

func (s *SumByPaymentFormat) handleRecord(clientID int, t transfer.Transfer) {
	method := t.PaymentFormat

	s.mu.Lock()
	if s.acumuladores[clientID] == nil {
		s.acumuladores[clientID] = map[string]transfer.SumByMethod{}
	}
	existing, ok := s.acumuladores[clientID][method]
	if !ok {
		s.acumuladores[clientID][method] = transfer.SumByMethod{
			Sum:    t.AmountPaid,
			Amount: 1,
			Method: method,
		}
		s.msgMonitor.AddForwardedMessagesAmountByClientId(clientID, 1)
	} else {
		s.acumuladores[clientID][method] = transfer.SumByMethod{
			Sum:    existing.Sum + t.AmountPaid,
			Amount: existing.Amount + 1,
			Method: method,
		}
	}
	s.mu.Unlock()
	s.msgMonitor.AddProcessedMessagesAmountByClientId(clientID, 1)
}

func (s *SumByPaymentFormat) handleEOF(data inner.DataMsg[transfer.Transfer]) {
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     data.EOF.TotalMessages,
		ActualAmount:   s.msgMonitor.GetProcessedMessagesAmountByClientId(data.ClientID),
		ClientId:       data.ClientID,
		CoordinatorId:  uint32(s.id),
		FilteredAmount: s.msgMonitor.GetForwardedMessagesAmountByClientId(data.ClientID),
	}
	serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
	if err != nil {
		slog.Error("While serializing EOF ring message", "err", err)
		return
	}
	if err := s.eofOutput.Send(*serialized); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
		return
	}
}

func (s *SumByPaymentFormat) onRingConverged(clientID int, msg *middleware.Message, isCoordinator bool) error {
	s.mu.Lock()
	byMethod := s.acumuladores[clientID]
	delete(s.acumuladores, clientID)
	s.mu.Unlock()

	for method, partial := range byMethod {
		out, err := inner.SerializeData(inner.DataMsg[transfer.SumByMethod]{
			Payload:  partial,
			ClientID: clientID,
			QueryID:  s.queryID,
		})
		if err != nil {
			return err
		}
		idx := shard.CalculateIndexForShard(clientID, method, len(s.outputQueues))
		if err := s.outputQueues[idx].Send(*out); err != nil {
			return err
		}
	}

	if !isCoordinator {
		return nil
	}
	for _, q := range s.outputQueues {
		if err := q.Send(*msg); err != nil {
			return err
		}
	}
	return nil
}
