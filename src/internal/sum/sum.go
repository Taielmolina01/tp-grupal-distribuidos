package sum

import (
	"log/slog"
	"strconv"
	"sync"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

type SumConfig struct {
	Id                int
	SumAmount         int
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueues      []string
	QueryID           uint8
	InputEofsExpected int
}


type SumByPaymentFormat struct {
	id              int
	inputQueue      middleware.Middleware
	outputQueues    []middleware.Middleware
	outputQueueEof  middleware.Middleware
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	queryID         uint8

	acumuladores map[int]map[string]transfer.SumByMethod
	lock         sync.Mutex
}

func CreateSumByPaymentFormat(config SumConfig) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, 0, len(config.OutputQueues))
	for _, q := range config.OutputQueues {
		m, err := middleware.CreateQueueMiddleware(q, connSettings)
		if err != nil {
			return nil, err
		}
		outputQueues = append(outputQueues, m)
	}

	next := config.Id + 1
	if config.Id == config.SumAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"SUM_"+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		"SUM_"+strconv.Itoa(next),
		connSettings,
	)
	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	sumWorker := &SumByPaymentFormat{
		id:              config.Id,
		inputQueue:      inputQueue,
		outputQueues:    outputQueues,
		outputQueueEof:  eofOutput,
		handlerMessages: handlerMessages,
		queryID:         config.QueryID,
		acumuladores:    map[int]map[string]transfer.SumByMethod{},
	}

	sumWorker.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.SumAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			sumWorker.lock.Lock()
			byMethod := sumWorker.acumuladores[clientID]
			delete(sumWorker.acumuladores, clientID)
			sumWorker.lock.Unlock()

			for method, partial := range byMethod {
				out, err := inner.SerializeData(inner.DataMsg[transfer.SumByMethod]{
					Payload:  partial,
					ClientID: clientID,
					QueryID:  sumWorker.queryID,
				})
				if err != nil {
					return err
				}
				idx := shard.CalculateIndexForShard(clientID, method, len(sumWorker.outputQueues))
				slog.Info("Sum sending partial to aggregate", "client_id", clientID, "method", method, "sum", partial.Sum, "amount", partial.Amount, "shard", idx)
				if err := sumWorker.outputQueues[idx].Send(*out); err != nil {
					return err
				}
			}

			if !isCoordinator {
				return nil
			}
			for _, q := range sumWorker.outputQueues {
				if err := q.Send(*msg); err != nil {
					return err
				}
			}
			return nil
		},
		config.QueryID,
	)

	return sumWorker, nil
}

func (s *SumByPaymentFormat) Run() {
	slog.Info("Starting sum-by-payment-format consumers", "sum_id", s.id)
	go s.eofHandler.Run()
	if err := s.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		s.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (s *SumByPaymentFormat) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.Transfer](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		ringMsg := eofmessagetypes.EofRingMessage{
			RealAmount:     result.EOF.TotalMessages,
			ActualAmount:   s.handlerMessages.GetProcessedMessagesAmountByClientId(result.ClientID),
			ClientId:       result.ClientID,
			CoordinatorId:  uint32(s.id),
			FilteredAmount: s.handlerMessages.GetFilteredMessagesAmountByClientId(result.ClientID),
		}
		serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
		if err != nil {
			slog.Error("While serializing EOF ring message", "err", err)
			return
		}
		if err := s.outputQueueEof.Send(*serialized); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
		}
		slog.Info("Sum fired ring", "sum_id", s.id, "client_id", result.ClientID, "real_amount", result.EOF.TotalMessages)
		return
	}

	t := result.Payload
	method := t.PaymentFormat

	s.lock.Lock()
	if s.acumuladores[result.ClientID] == nil {
		s.acumuladores[result.ClientID] = map[string]transfer.SumByMethod{}
	}
	existing, ok := s.acumuladores[result.ClientID][method]
	if !ok {
		s.acumuladores[result.ClientID][method] = transfer.SumByMethod{
			Sum:    t.AmountPaid,
			Amount: 1,
			Method: method,
		}
		s.handlerMessages.AddFilteredMessagesAmountByClientId(result.ClientID, 1)
	} else {
		s.acumuladores[result.ClientID][method] = transfer.SumByMethod{
			Sum:    existing.Sum + t.AmountPaid,
			Amount: existing.Amount + 1,
			Method: method,
		}
	}
	s.lock.Unlock()
	s.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
}

func (s *SumByPaymentFormat) HandleSignals() {
}
