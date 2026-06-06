package daterangesplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"tp-grupal-distribuidos/internal/common/eofmessagetypes"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/daterange"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const eofRingPrefix = "DATE_RANGE_SPLITTER_"

type DateRangeSplitterConfig struct {
	Id             int
	SplitterAmount int

	MomHost string
	MomPort int

	InputExchange    string
	InputQueue       string
	InputRoutingKeys []string

	OutputQueues []string

	QueryID uint8

	AvgPeriodStart    time.Time
	AvgPeriodEnd      time.Time
	FilterPeriodStart time.Time
	FilterPeriodEnd   time.Time
}

type DateRangeSplitter struct {
	id uint32

	inputExchange middleware.Middleware

	outputQueues []middleware.Middleware
	monitors     []msgmonitor.MessageMonitor
	eofInputs    []middleware.Middleware
	eofOutputs   []middleware.Middleware
	eofHandlers  []eofring.EofRingAlgorithm

	queryID uint8

	avgPeriodStart, avgPeriodEnd       time.Time
	filterPeriodStart, filterPeriodEnd time.Time
}

func getRingNextIndex(config DateRangeSplitterConfig) int {
	if config.Id == config.SplitterAmount-1 {
		return 0
	}
	return config.Id + 1
}

func NewDateRangeSplitter(config DateRangeSplitterConfig) (_ *DateRangeSplitter, err error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputExchange middleware.Middleware
		outputQueues  []middleware.Middleware
		eofInputs     []middleware.Middleware
		eofOutputs    []middleware.Middleware
	)

	defer func() {
		if err == nil {
			return
		}
		for _, q := range eofOutputs {
			q.Close()
		}
		for _, q := range eofInputs {
			q.Close()
		}
		for _, q := range outputQueues {
			q.Close()
		}
		if inputExchange != nil {
			inputExchange.Close()
		}
	}()

	inputExchange, err = middleware.CreateExchangeMiddleware(
		config.InputExchange,
		config.InputQueue,
		config.InputRoutingKeys,
		connSettings,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input exchange: %w", err)
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

	next := getRingNextIndex(config)
	monitors := make([]msgmonitor.MessageMonitor, 0, len(outputQueues))
	eofInputs = make([]middleware.Middleware, 0, len(outputQueues))
	eofOutputs = make([]middleware.Middleware, 0, len(outputQueues))

	for idx := range outputQueues {
		eofIn, e := middleware.CreateQueueMiddleware(
			eofRingPrefix+strconv.Itoa(idx)+"_"+strconv.Itoa(config.Id),
			connSettings,
		)
		if e != nil {
			err = fmt.Errorf("creating EOF input queue: %w", e)
			return nil, err
		}
		eofOut, e := middleware.CreateQueueMiddleware(
			eofRingPrefix+strconv.Itoa(idx)+"_"+strconv.Itoa(next),
			connSettings,
		)
		if e != nil {
			eofIn.Close()
			err = fmt.Errorf("creating EOF output queue: %w", e)
			return nil, err
		}

		eofInputs = append(eofInputs, eofIn)
		eofOutputs = append(eofOutputs, eofOut)
		monitors = append(monitors, msgmonitor.NewMessageMonitor())
	}

	s := &DateRangeSplitter{
		id:                uint32(config.Id),
		inputExchange:     inputExchange,
		outputQueues:      outputQueues,
		monitors:          monitors,
		eofInputs:         eofInputs,
		eofOutputs:        eofOutputs,
		queryID:           config.QueryID,
		avgPeriodStart:    config.AvgPeriodStart,
		avgPeriodEnd:      config.AvgPeriodEnd,
		filterPeriodStart: config.FilterPeriodStart,
		filterPeriodEnd:   config.FilterPeriodEnd,
	}

	eofHandlers := make([]eofring.EofRingAlgorithm, 0, len(outputQueues))
	for idx := range outputQueues {
		eofHandlers = append(eofHandlers, eofring.CreateEofRingAlgorithm(
			eofInputs[idx],
			eofOutputs[idx],
			config.SplitterAmount,
			uint32(config.Id),
			monitors[idx],
			func(clientID int, total uint32, isCoordinator bool) error {
				if !isCoordinator {
					return nil
				}
				if err := s.outputQueues[idx].Send(middleware.Message{Body: string(batch.WriteEOF(clientID, s.queryID, 0, 0, total))}); err != nil {
					return err
				}
				return nil
			},
			config.QueryID,
		))
	}
	s.eofHandlers = eofHandlers

	return s, nil
}

func (s *DateRangeSplitter) Run() {
	defer s.close()
	slog.Info("Starting date range splitter consumers", "splitter_id", s.id)
	for _, h := range s.eofHandlers {
		go h.Run()
	}
	if err := s.inputExchange.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		s.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (s *DateRangeSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	s.inputExchange.StopConsuming()
	for _, q := range s.eofInputs {
		q.StopConsuming()
	}
}

func (s *DateRangeSplitter) close() {
	s.inputExchange.Close()
	for _, q := range s.eofInputs {
		q.Close()
	}
	for _, q := range s.eofOutputs {
		q.Close()
	}
	for _, q := range s.outputQueues {
		q.Close()
	}
}

func (s *DateRangeSplitter) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), records.TransferAfterCurrencyCodec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		s.handleEOF(input.ClientID, input.Total)
		return
	}

	s.handleBatch(input.ClientID, input.Records)
}

func (s *DateRangeSplitter) handleBatch(clientID int, recs []transfer.TransferAfterCurrency) {
	var avg []transfer.TransferForQ3Avg
	var filter []transfer.TransferForQ3Filter

	for i := range recs {
		record := recs[i]
		for _, m := range s.monitors {
			m.AddProcessedMessagesAmountByClientId(clientID, 1)
		}

		idx := s.periodIndex(record)
		if idx < 0 || idx >= len(s.outputQueues) {
			continue
		}
		s.monitors[idx].AddForwardedMessagesAmountByClientId(clientID, 1)

		switch idx {
		case 0:
			avg = append(avg, transfer.ProjectForQ3Avg(record))
		case 1:
			filter = append(filter, transfer.ProjectForQ3Filter(record))
		}
	}

	if len(avg) > 0 {
		if err := s.outputQueues[0].Send(middleware.Message{Body: string(daterange.WriteBatch(clientID, s.queryID, 0, 0, avg))}); err != nil {
			slog.Error("While sending Q3 avg batch", "err", err)
		}
	}
	if len(filter) > 0 {
		if err := s.outputQueues[1].Send(middleware.Message{Body: string(q3filter.WriteBatch(clientID, s.queryID, 0, 0, filter))}); err != nil {
			slog.Error("While sending Q3 filter batch", "err", err)
		}
	}
}

func (s *DateRangeSplitter) handleEOF(clientID int, total uint32) {
	for idx, eofOut := range s.eofOutputs {
		ringMsg := eofmessagetypes.EofRingMessage{
			RealAmount:     total,
			ActualAmount:   s.monitors[idx].GetProcessedMessagesAmountByClientId(clientID),
			ClientId:       clientID,
			CoordinatorId:  uint32(s.id),
			FilteredAmount: s.monitors[idx].GetForwardedMessagesAmountByClientId(clientID),
		}
		if err := eofOut.Send(middleware.Message{Body: string(eofring.SerializeRingMessage(ringMsg))}); err != nil {
			slog.Error("While sending EOF message to EOF ring", "err", err)
			continue
		}
		slog.Info("DateRangeSplitter fired ring", "splitter_id", s.id, "client_id", clientID, "output_idx", idx)
	}
}

func (s *DateRangeSplitter) periodIndex(t transfer.TransferAfterCurrency) int {
	switch {
	case !t.Timestamp.Before(s.avgPeriodStart) && !t.Timestamp.After(s.avgPeriodEnd):
		return 0
	case !t.Timestamp.Before(s.filterPeriodStart) && !t.Timestamp.After(s.filterPeriodEnd):
		return 1
	default:
		return -1
	}
}
