package sum

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
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/summethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewSumByPaymentFormat(config SumConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			cleanup.Close(outputMiddleware, inputMiddleware)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	shardKey := fmt.Sprintf("shard-%d", config.Id)
	inputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, shardKey)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = newmiddleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalClientState, unmarshalClientState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState {
		return &clientState{
			tracker:      sendertracker.New(10_000_000),
			acumuladores: map[string]transfer.SumByMethod{},
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &SumByPaymentFormat{
		id:                   config.Id,
		queryID:              config.QueryID,
		inputMiddleware:      inputMiddleware,
		outputMiddleware:     outputMiddleware,
		prevNodeAmt:          config.ExpectedEOFs,
		outputAmount:         config.OutputAmount,
		hasher:               shard.New(config.OutputAmount),
		maxBatchSize:         config.MaxBatchSize,
		maxBatchBytes:        config.MaxBatchBytes,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (s *SumByPaymentFormat) Run() {
	defer s.close()
	slog.Info("Starting sum-by-payment-format consumers", "sum_id", s.id)
	if err := s.inputMiddleware.StartConsumingBatch(s.persistBatchSize, s.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		s.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (s *SumByPaymentFormat) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	s.stopConsuming()
}

func (s *SumByPaymentFormat) stopConsuming() {
	if err := s.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "sum_id", s.id, "err", err)
	}
}

func (s *SumByPaymentFormat) close() {
	cleanup.Close(s.inputMiddleware, s.outputMiddleware)
}

func (s *SumByPaymentFormat) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := daterange.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := s.states.For(clientID)
		tracker := state.tracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			s.processRecords(input.Records, state)
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

func (s *SumByPaymentFormat) processRecords(records []transfer.TransferForQ3Avg, state *clientState) {
	for _, t := range records {
		method := t.PaymentFormat
		existing, ok := state.acumuladores[method]
		if !ok {
			state.acumuladores[method] = transfer.SumByMethod{
				Sum:    t.AmountPaid,
				Amount: 1,
				Method: method,
			}
		} else {
			state.acumuladores[method] = transfer.SumByMethod{
				Sum:    existing.Sum + t.AmountPaid,
				Amount: existing.Amount + 1,
				Method: method,
			}
		}
	}
}

func (s *SumByPaymentFormat) finishStep(clientID int, state *clientState) error {
	ot := outputtracker.New()
	builders := make(map[string]*batch.Builder[transfer.SumByMethod])

	for method, partial := range state.acumuladores {
		rk := fmt.Sprintf("shard-%d", s.hasher.ShardFor(clientID, method))
		b := s.builderFor(builders, rk)
		if !b.TryAdd(&partial) {
			seq := ot.RegisterBatch(rk)
			if err := s.flushBatch(clientID, rk, seq, b); err != nil {
				return err
			}
			b.TryAdd(&partial)
		}
	}

	for rk, b := range builders {
		if !b.IsEmpty() {
			seq := ot.RegisterBatch(rk)
			if err := s.flushBatch(clientID, rk, seq, b); err != nil {
				return err
			}
		}
	}

	for i := range s.outputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := ot.CountFor(rk)
		seq := ot.RegisterBatch(rk)
		if err := msgsend.SendEOF(s.outputMiddleware, rk, clientID, s.queryID, uint8(s.id), seq, uint32(total)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SumByPaymentFormat) builderFor(builders map[string]*batch.Builder[transfer.SumByMethod], rk string) *batch.Builder[transfer.SumByMethod] {
	b := builders[rk]
	if b == nil {
		b = summethod.NewBatchBuilder(s.maxBatchSize, s.maxBatchBytes)
		builders[rk] = b
	}
	return b
}

func (s *SumByPaymentFormat) flushBatch(clientID int, rk string, seq uint64, b *batch.Builder[transfer.SumByMethod]) error {
	body := b.Flush(clientID, s.queryID, uint8(s.id), seq)
	return s.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk})
}
