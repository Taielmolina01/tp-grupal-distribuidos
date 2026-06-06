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
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/daterange"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/summethod"
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

	if err := s.inputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping input queue consumer", "sum_id", s.id, "err", err)
	}
	if err := s.eofInput.StopConsuming(); err != nil {
		slog.Error("While stopping EOF input consumer", "sum_id", s.id, "err", err)
	}
}

func (s *SumByPaymentFormat) close() {
	if err := s.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "sum_id", s.id, "err", err)
	}
	if err := s.eofInput.Close(); err != nil {
		slog.Error("While closing EOF input", "sum_id", s.id, "err", err)
	}
	if err := s.eofOutput.Close(); err != nil {
		slog.Error("While closing EOF output", "sum_id", s.id, "err", err)
	}
	for _, q := range s.outputQueues {
		if err := q.Close(); err != nil {
			slog.Error("While closing output queue", "sum_id", s.id, "err", err)
		}
	}
}

func (s *SumByPaymentFormat) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	input, err := daterange.Read([]byte(msg.Body))
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		s.handleEOF(input.ClientID, input.Total)
		return
	}

	for i := range input.Records {
		s.handleRecord(input.ClientID, input.Records[i])
	}
}

func (s *SumByPaymentFormat) handleRecord(clientID int, t transfer.TransferForQ3Avg) {
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

func (s *SumByPaymentFormat) handleEOF(clientID int, total uint32) {
	ringMsg := eofmessagetypes.EofRingMessage{
		RealAmount:     total,
		ActualAmount:   s.msgMonitor.GetProcessedMessagesAmountByClientId(clientID),
		ClientId:       clientID,
		CoordinatorId:  uint32(s.id),
		FilteredAmount: s.msgMonitor.GetForwardedMessagesAmountByClientId(clientID),
	}
	if err := s.eofOutput.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(ringMsg))}); err != nil {
		slog.Error("While sending EOF message to EOF ring", "err", err)
		return
	}
}

func (s *SumByPaymentFormat) onRingConverged(clientID int, total uint32, isCoordinator bool) error {
	s.mu.Lock()
	byMethod := s.acumuladores[clientID]
	delete(s.acumuladores, clientID)
	s.mu.Unlock()

	byShard := make(map[int][]transfer.SumByMethod)
	for method, partial := range byMethod {
		idx := shard.CalculateIndexForShard(clientID, method, len(s.outputQueues))
		byShard[idx] = append(byShard[idx], partial)
	}
	for idx, group := range byShard {
		body := summethod.WriteBatch(clientID, s.queryID, group)
		if err := s.outputQueues[idx].Send(middleware.Message{Body: string(body)}); err != nil {
			return err
		}
	}

	if !isCoordinator {
		return nil
	}
	eofBody := summethod.WriteEOF(clientID, s.queryID, total)
	for _, q := range s.outputQueues {
		if err := q.Send(middleware.Message{Body: string(eofBody)}); err != nil {
			return err
		}
	}
	return nil
}
