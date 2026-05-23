package filter

import (
	"log/slog"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

type DateRangeSplitter struct {
	id              uint32
	inputExchange   middleware.Middleware
	outputQueues    []middleware.Middleware
	outputQueueEof  middleware.Middleware
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	queryID         uint8

	avgPeriodStart, avgPeriodEnd time.Time
	filterPeriodStart, filterPeriodEnd time.Time
}

func CreateDateAndRangeSplitter(config FilterConfig) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)
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
	if config.Id == config.FilterAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateQueueMiddleware(
		"DATE_RANGE_SPLITTER_"+strconv.Itoa(config.Id),
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	eofOutput, err := middleware.CreateQueueMiddleware(
		"DATE_RANGE_SPLITTER_"+strconv.Itoa(next),
		connSettings,
	)
	if err != nil {
		eofInput.Close()
		return nil, err
	}

	handlerMessages := msgmonitor.NewMessageMonitor()

	splitter := &DateRangeSplitter{
		id:              uint32(config.Id),
		inputExchange:   inputExchange,
		outputQueues:    outputQueues,
		outputQueueEof:  eofOutput,
		handlerMessages: handlerMessages,
		queryID:         inner.Query3ID,
		avgPeriodStart:    time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC),
		avgPeriodEnd:      time.Date(2022, 9, 5, 23, 59, 59, 0, time.UTC),
		filterPeriodStart: time.Date(2022, 9, 6, 0, 0, 0, 0, time.UTC),
		filterPeriodEnd:   time.Date(2022, 9, 15, 23, 59, 59, 0, time.UTC),
	}

	splitter.eofHandler = eofring.CreateEofRingAlgorithm(
		eofInput,
		eofOutput,
		config.FilterAmount,
		uint32(config.Id),
		handlerMessages,
		func(clientID int, msg *middleware.Message, isCoordinator bool) error {
			if !isCoordinator {
				return nil
			}
			for _, q := range splitter.outputQueues {
				if err := q.Send(*msg); err != nil {
					return err
				}
			}
			return nil
		},
		inner.Query3ID,
	)

	return splitter, nil
}

func (s *DateRangeSplitter) Run() {
	slog.Info("Starting date range splitter consumers", "splitter_id", s.id)
	go s.eofHandler.Run()
	if err := s.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		s.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (s *DateRangeSplitter) handleMessage(msg middleware.Message, ack, nack func()) {
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
		slog.Info("DateRangeSplitter sent EOF to ring", "splitter_id", s.id, "client_id", result.ClientID)
		return
	}

	s.handlerMessages.AddProcessedMessagesAmountByClientId(result.ClientID, 1)

	idx := s.periodIndex(result.Payload)
	if idx < 0 {
		return
	}
	if idx >= len(s.outputQueues) {
		slog.Error("DateRangeSplitter: route index out of range", "idx", idx, "outputs", len(s.outputQueues))
		return
	}

	s.handlerMessages.AddFilteredMessagesAmountByClientId(result.ClientID, 1)
	if err := s.outputQueues[idx].Send(msg); err != nil {
		slog.Error("While sending message to output queue", "idx", idx, "err", err)
	}
}

// periodIndex devuelve:
//
//	0 → la fecha cae en el período de cálculo del promedio (output queue 0).
//	1 → la fecha cae en el período a filtrar (output queue 1).
//	-1 → fuera de ambos rangos: descartar.
func (s *DateRangeSplitter) periodIndex(t transfer.Transfer) int {
	switch {
	case !t.Timestamp.Before(s.avgPeriodStart) && !t.Timestamp.After(s.avgPeriodEnd):
		return 0
	case !t.Timestamp.Before(s.filterPeriodStart) && !t.Timestamp.After(s.filterPeriodEnd):
		return 1
	default:
		return -1
	}
}

func (s *DateRangeSplitter) HandleSignals() {
}
