package filter

import (
	"log/slog"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

// DateRangeSplitter (Q3): rutea cada Transfer al output 0 si la fecha cae en
// [09-01, 09-05], al output 1 si cae en [09-06, 09-15], y descarta el resto.
//
// Diseño de EOFs: hay UN ring de coordinación POR OUTPUT QUEUE. Cada ring
// tiene su propio MessageMonitor: todos los monitors registran el mismo
// "processed" (total de inputs leídos por la réplica), pero el "filtered"
// es específico — solo se incrementa cuando el mensaje sale por ESA queue.
// Así cuando cada ring commitea, el FilteredAmount global representa la
// cantidad real de mensajes que llegaron a esa queue específica, y se
// puede emitir un EOF downstream con el TotalMessages correcto.
type DateRangeSplitter struct {
	id            uint32
	inputExchange newmiddleware.Middleware

	// Por output queue:
	outputQueues    []newmiddleware.Middleware
	monitors        []msgmonitor.MessageMonitor
	eofHandlers     []eofring.EofRingAlgorithm
	outputQueueEofs []newmiddleware.Middleware

	queryID uint8

	avgPeriodStart, avgPeriodEnd       time.Time
	filterPeriodStart, filterPeriodEnd time.Time
}

func CreateDateAndRangeSplitter(config FilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := newmiddleware.NewFanoutMiddleware(
		connSettings,
		config.InputExchange,
		config.InputQueue,
	)
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
	if config.Id == config.FilterAmount-1 {
		next = 0
	}

	// Por cada output queue: un ring (input/output queues), un monitor, un eof handler.
	monitors := make([]msgmonitor.MessageMonitor, 0, len(outputQueues))
	eofInputs := make([]newmiddleware.Middleware, 0, len(outputQueues))
	eofOutputs := make([]newmiddleware.Middleware, 0, len(outputQueues))

	for idx := range outputQueues {
		eofIn, err := newmiddleware.NewQueueMiddleware(
			connSettings,
			"DATE_RANGE_SPLITTER_"+strconv.Itoa(idx)+"_"+strconv.Itoa(config.Id),
		)
		if err != nil {
			return nil, err
		}
		eofOut, err := newmiddleware.NewQueueMiddleware(
			connSettings,
			"DATE_RANGE_SPLITTER_"+strconv.Itoa(idx)+"_"+strconv.Itoa(next),
		)
		if err != nil {
			eofIn.Close()
			return nil, err
		}

		eofInputs = append(eofInputs, eofIn)
		eofOutputs = append(eofOutputs, eofOut)
		monitors = append(monitors, msgmonitor.NewMessageMonitor())
	}

	splitter := &DateRangeSplitter{
		id:                uint32(config.Id),
		inputExchange:     inputExchange,
		outputQueues:      outputQueues,
		monitors:          monitors,
		outputQueueEofs:   eofOutputs,
		queryID:           inner.Query3ID,
		avgPeriodStart:    time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC),
		avgPeriodEnd:      time.Date(2022, 9, 5, 23, 59, 59, 0, time.UTC),
		filterPeriodStart: time.Date(2022, 9, 6, 0, 0, 0, 0, time.UTC),
		filterPeriodEnd:   time.Date(2022, 9, 15, 23, 59, 59, 0, time.UTC),
	}

	eofHandlers := make([]eofring.EofRingAlgorithm, 0, len(outputQueues))
	for idx := range outputQueues {
		idx := idx // capture
		eofHandlers = append(eofHandlers, eofring.CreateEofRingAlgorithm(
			eofInputs[idx],
			eofOutputs[idx],
			config.FilterAmount,
			uint32(config.Id),
			monitors[idx],
			func(clientID int, msg *newmiddleware.Message, isCoordinator bool) error {
				if !isCoordinator {
					return nil
				}
				if err := splitter.outputQueues[idx].Send(*msg); err != nil {
					return err
				}
				slog.Info("Splitter emitted EOF to output queue", "splitter_id", splitter.id, "client_id", clientID, "output_idx", idx)
				return nil
			},
			inner.Query3ID,
		))
	}
	splitter.eofHandlers = eofHandlers

	return splitter, nil
}

func (s *DateRangeSplitter) Run() {
	slog.Info("Starting date range splitter consumers", "splitter_id", s.id)
	for _, h := range s.eofHandlers {
		go h.Run()
	}
	if err := s.inputExchange.StartConsuming(func(msg newmiddleware.Message, ack, nack func()) {
		s.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (s *DateRangeSplitter) handleMessage(msg newmiddleware.Message, ack, nack func()) {
	defer ack()

	result, err := inner.DeserializeData[transfer.Transfer](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if result.IsEOF() {
		// Disparamos UN ring por cada output queue. Cada ring va a converger por
		// su lado y emitir el EOF correcto a su output downstream.
		for idx, eofOut := range s.outputQueueEofs {
			ringMsg := eofmessagetypes.EofRingMessage{
				RealAmount:     result.EOF.TotalMessages,
				ActualAmount:   s.monitors[idx].GetProcessedMessagesAmountByClientId(result.ClientID),
				ClientId:       result.ClientID,
				CoordinatorId:  uint32(s.id),
				FilteredAmount: s.monitors[idx].GetForwardedMessagesAmountByClientId(result.ClientID),
			}
			serialized, err := inner.SerializeEofFromQueueMsg(ringMsg)
			if err != nil {
				slog.Error("While serializing EOF ring message", "err", err)
				continue
			}
			if err := eofOut.Send(*serialized); err != nil {
				slog.Error("While sending EOF message to EOF ring", "err", err)
				continue
			}
			slog.Info("DateRangeSplitter fired ring", "splitter_id", s.id, "client_id", result.ClientID, "output_idx", idx)
		}
		return
	}

	// Processed se incrementa en TODOS los monitors (representa total leído por
	// esta réplica del input — igual para todos).
	for _, m := range s.monitors {
		m.AddProcessedMessagesAmountByClientId(result.ClientID, 1)
	}

	idx := s.periodIndex(result.Payload)
	if idx < 0 {
		return
	}
	if idx >= len(s.outputQueues) {
		slog.Error("DateRangeSplitter: route index out of range", "idx", idx, "outputs", len(s.outputQueues))
		return
	}

	// Filtered solo se incrementa en el monitor de la queue destino.
	s.monitors[idx].AddForwardedMessagesAmountByClientId(result.ClientID, 1)

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
