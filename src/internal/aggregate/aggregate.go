package aggregate

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/avgmethod"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/summethod"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewAvgAggregator(config AggregateConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  newmiddleware.Middleware
		outputMiddleware newmiddleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			for _, m := range []newmiddleware.Middleware{outputMiddleware, inputMiddleware} {
				if m != nil {
					if closeErr := m.Close(); closeErr != nil {
						slog.Error("While closing middleware", "err", closeErr)
					}
				}
			}
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
			acumuladores: map[string]partial{},
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID)
	}

	return &AvgAggregator{
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

func (a *AvgAggregator) Run() {
	defer a.close()
	slog.Info("Starting avg-aggregator consumers", "aggregate_id", a.id)
	if err := a.inputMiddleware.StartConsumingBatch(a.persistBatchSize, a.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
		a.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (a *AvgAggregator) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	a.stopConsuming()
}

func (a *AvgAggregator) stopConsuming() {
	if err := a.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "aggregate_id", a.id, "err", err)
	}
}

func (a *AvgAggregator) close() {
	if err := a.inputMiddleware.Close(); err != nil {
		slog.Error("While closing input middleware", "aggregate_id", a.id, "err", err)
	}
	if err := a.outputMiddleware.Close(); err != nil {
		slog.Error("While closing output middleware", "aggregate_id", a.id, "err", err)
	}
}

func (a *AvgAggregator) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
	modified := make(map[int]*clientState)
	completed := make(map[int]struct{})

	for _, msg := range msgs {
		input, err := summethod.Read(msg.Body)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := a.states.For(clientID)
		tracker := state.tracker

		if tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			tracker.RegisterEOF(int(input.SenderID), uint64(input.Total), input.Seq)
		} else {
			tracker.RegisterBatch(int(input.SenderID))
			a.processRecords(input.Records, state)
		}

		tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if tracker.IsComplete(a.prevNodeAmt) {
			if err := a.finishStep(clientID, state); err != nil {
				slog.Error("finish step failed", "err", err)
				nack()
				a.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if _, done := completed[clientID]; done {
			a.states.Delete(clientID)
			a.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := a.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			a.stopConsuming()
			return
		}
	}

	ack()
}

func (a *AvgAggregator) processRecords(records []transfer.SumByMethod, state *clientState) {
	for _, p := range records {
		existing, ok := state.acumuladores[p.Method]
		if !ok {
			state.acumuladores[p.Method] = partial{
				totalSum:   p.Sum,
				totalCount: p.Amount,
			}
		} else {
			state.acumuladores[p.Method] = partial{
				totalSum:   existing.totalSum + p.Sum,
				totalCount: existing.totalCount + p.Amount,
			}
		}
	}
}

func (a *AvgAggregator) finishStep(clientID int, state *clientState) error {
	ot := outputtracker.New()
	builders := make(map[string]*batch.Builder[transfer.AvgByMethod])

	for method, p := range state.acumuladores {
		if p.totalCount == 0 {
			continue
		}
		avg := transfer.AvgByMethod{
			Method: method,
			Avg:    p.totalSum / float64(p.totalCount),
		}
		rk := fmt.Sprintf("shard-%d", a.hasher.ShardFor(clientID, method))
		b := a.builderFor(builders, rk)
		if !b.TryAdd(&avg) {
			seq := ot.RegisterBatch(rk)
			if err := a.flushBatch(clientID, rk, seq, b); err != nil {
				return err
			}
			b.TryAdd(&avg)
		}
	}

	for rk, b := range builders {
		if !b.IsEmpty() {
			seq := ot.RegisterBatch(rk)
			if err := a.flushBatch(clientID, rk, seq, b); err != nil {
				return err
			}
		}
	}

	for i := range a.outputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := ot.CountFor(rk)
		seq := ot.RegisterBatch(rk)
		eofBody := avgmethod.WriteEOF(clientID, a.queryID, uint8(a.id), seq, uint32(total))
		if err := a.outputMiddleware.Send(newmiddleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			return err
		}
	}
	return nil
}

func (a *AvgAggregator) builderFor(builders map[string]*batch.Builder[transfer.AvgByMethod], rk string) *batch.Builder[transfer.AvgByMethod] {
	b := builders[rk]
	if b == nil {
		b = avgmethod.NewBatchBuilder(a.maxBatchSize, a.maxBatchBytes)
		builders[rk] = b
	}
	return b
}

func (a *AvgAggregator) flushBatch(clientID int, rk string, seq uint64, b *batch.Builder[transfer.AvgByMethod]) error {
	body := b.Flush(clientID, a.queryID, uint8(a.id), seq)
	return a.outputMiddleware.Send(newmiddleware.Message{Body: body, RoutingKey: rk})
}
