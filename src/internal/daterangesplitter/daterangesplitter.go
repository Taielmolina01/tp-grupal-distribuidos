package daterangesplitter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/daterange"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/q3filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewDateRangeSplitter(config DateRangeSplitterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputExchange    newmiddleware.Middleware
		avgMiddleware    newmiddleware.Middleware
		filterMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			cleanup.Close(filterMiddleware, avgMiddleware, inputExchange)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputExchange, err = newmiddleware.NewShardedMiddleware(
		connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey,
	)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	avgMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.AvgOutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating avg output middleware: %w", err)
	}

	filterMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.FilterOutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating filter output middleware: %w", err)
	}

	ckpt, e := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if e != nil {
		err = e
		return nil, fmt.Errorf("creating checkpoint: %w", e)
	}

	recovered, e := ckpt.Load()
	if e != nil {
		err = e
		return nil, fmt.Errorf("loading checkpoint: %w", e)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			tracker:       sendertracker.New(10_000_000),
			outputTracker: outputtracker.New(),
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &DateRangeSplitter{
		id:                   uint32(config.Id),
		inputExchange:        inputExchange,
		avgMiddleware:        avgMiddleware,
		filterMiddleware:     filterMiddleware,
		avgOutputAmount:      config.AvgOutputAmount,
		filterOutputAmount:   config.FilterOutputAmount,
		avgHasher:            shard.New(config.AvgOutputAmount),
		filterHasher:         shard.New(config.FilterOutputAmount),
		prevNodeAmt:          config.ExpectedEOFs,
		queryID:              config.QueryID,
		avgPeriodStart:       config.AvgPeriodStart,
		avgPeriodEnd:         config.AvgPeriodEnd,
		filterPeriodStart:    config.FilterPeriodStart,
		filterPeriodEnd:      config.FilterPeriodEnd,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (s *DateRangeSplitter) Run() {
	defer s.close()

	if err := s.inputExchange.StartConsumingBatch(s.persistBatchSize, s.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		s.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (s *DateRangeSplitter) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	s.stopConsuming()
}

func (s *DateRangeSplitter) stopConsuming() {
	if err := s.inputExchange.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "splitter_id", s.id, "err", err)
	}
}

func (s *DateRangeSplitter) close() {
	cleanup.Close(s.inputExchange, s.avgMiddleware, s.filterMiddleware)
}

func (s *DateRangeSplitter) handleBatch(msgs []newmiddleware.Message, ack func(), nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})
	aborted := make(map[int]bool)

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, records.TransferAfterCurrencyCodec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := s.states.For(clientID)
		tracker := state.tracker

		if input.Abort || aborted[clientID] {
			aborted[clientID] = true
			modified[clientID] = state
			continue
		}

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			if err := s.processBatch(input, state); err != nil {
				slog.Error("process batch failed", "err", err)
				nack()
				s.stopConsuming()
				return
			}
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(s.prevNodeAmt) {
			if err := s.finishStep(clientID, state); err != nil {
				slog.Error("finish step failed", "err", err)
				nack()
				s.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if aborted[clientID] {
			if err := msgsend.SendAbort(s.avgMiddleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
				slog.Error("While emitting abort", "err", err)
				nack()
				s.stopConsuming()
				return
			}
			if err := msgsend.SendAbort(s.filterMiddleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
				slog.Error("While emitting abort", "err", err)
				nack()
				s.stopConsuming()
				return
			}
			s.states.Delete(clientID)
			s.checkpoint.DeleteClient(clientID)
			continue
		}
		if _, done := completed[clientID]; done {
			s.states.Delete(clientID)
			s.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := s.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			s.stopConsuming()
			return
		}
	}

	ack()
}

func (s *DateRangeSplitter) processBatch(input batch.Msg[transfer.TransferAfterCurrency], state *clientState) error {
	byAvgShard := make(map[string][]transfer.TransferForQ3Avg)
	byFilterShard := make(map[string][]transfer.TransferForQ3Filter)

	for _, record := range input.Records {
		switch s.periodIndex(record) {
		case 0:
			rk := fmt.Sprintf("shard-%d", s.avgHasher.ShardFor(input.ClientID, record.PaymentFormat))
			byAvgShard[rk] = append(byAvgShard[rk], transfer.ProjectForQ3Avg(record))
		case 1:
			rk := fmt.Sprintf("shard-%d", s.filterHasher.ShardFor(input.ClientID, record.PaymentFormat))
			byFilterShard[rk] = append(byFilterShard[rk], transfer.ProjectForQ3Filter(record))
		}
	}

	for rk, group := range byAvgShard {
		body := daterange.WriteBatch(input.ClientID, s.queryID, uint8(s.id), input.Seq, group)
		if err := s.avgMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			return err
		}
		state.outputTracker.RegisterBatch("avg_" + rk)
	}
	for rk, group := range byFilterShard {
		body := q3filter.WriteBatch(input.ClientID, s.queryID, uint8(s.id), input.Seq, group)
		if err := s.filterMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk}); err != nil {
			return err
		}
		state.outputTracker.RegisterBatch("filter_" + rk)
	}
	return nil
}

func (s *DateRangeSplitter) finishStep(clientID int, state *clientState) error {
	eofSeq := state.tracker.GetEOFSeq()

	for i := range s.avgOutputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := state.outputTracker.CountFor("avg_" + rk)
		if err := msgsend.SendEOF(s.avgMiddleware, rk, clientID, s.queryID, uint8(s.id), eofSeq, uint32(total)); err != nil {
			return err
		}
	}

	for i := range s.filterOutputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := state.outputTracker.CountFor("filter_" + rk)
		if err := msgsend.SendEOF(s.filterMiddleware, rk, clientID, s.queryID, uint8(s.id), eofSeq, uint32(total)); err != nil {
			return err
		}
	}

	return nil
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
