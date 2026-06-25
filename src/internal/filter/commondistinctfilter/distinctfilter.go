package commondistinctfilter

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/cleanup"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/msgsend"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/worker"
)

func NewDistinctFilter[T comparable](
	config filter.FilterConfig,
	keyFunc func(T) string,
	codec wire.Codec[T],
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware  middleware.Middleware
		outputMiddleware middleware.Middleware
		err              error
	)

	defer func() {
		if err != nil {
			cleanup.Close(inputMiddleware, outputMiddleware)
		}
	}()

	inputQueue := config.InputMiddlewarePrefix + "_" + strconv.Itoa(config.Id)
	inputMiddleware, err = middleware.NewShardedMiddleware(connSettings, config.InputMiddlewarePrefix, inputQueue, fmt.Sprintf("shard-%d", config.Id))
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	outputMiddleware, err = middleware.NewShardedMiddleware(connSettings, config.OutputMiddlewarePrefix, "", "")
	if err != nil {
		return nil, fmt.Errorf("creating output middleware: %w", err)
	}

	marshalState := func(s *clientState[T]) []byte {
		w := wire.NewWriter()
		s.tracker.Marshal(w)
		w.Uint32(uint32(len(s.seen)))
		for key, item := range s.seen {
			w.String(key)
			codec.Marshal(w, &item)
		}
		return w.Bytes()
	}
	unmarshalState := func(data []byte) (*clientState[T], error) {
		r := wire.NewReader(data)
		tracker, terr := sendertracker.Unmarshal(r)
		if terr != nil {
			return nil, fmt.Errorf("distinctfilter: unmarshal tracker: %w", terr)
		}
		n := r.Uint32()
		if r.Err() != nil {
			return nil, fmt.Errorf("distinctfilter: unmarshal header: %w", r.Err())
		}
		seen := make(map[string]T, n)
		for range n {
			key := r.String()
			item := codec.Unmarshal(r)
			if r.Err() != nil {
				return nil, fmt.Errorf("distinctfilter: unmarshal entry: %w", r.Err())
			}
			seen[key] = item
		}
		if r.Err() != nil {
			return nil, fmt.Errorf("distinctfilter: unmarshal: %w", r.Err())
		}
		return &clientState[T]{tracker: tracker, seen: seen}, nil
	}

	ckpt, err := checkpoint.New(config.PersistPath, marshalState, unmarshalState)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	recovered, err := ckpt.Load()
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	states := statemap.New(func() *clientState[T] {
		return &clientState[T]{
			tracker: sendertracker.New(10_000_000),
			seen:    map[string]T{},
		}
	})
	for clientID, state := range recovered {
		states.Set(clientID, state)
		slog.Info("recovered client state", "clientID", clientID, "seen", len(state.seen))
	}

	return &DistinctFilter[T]{
		id:                   uint32(config.Id),
		queryId:              config.QueryID,
		expectedEOFs:         1,
		inputMiddleware:      inputMiddleware,
		outputMiddleware:     outputMiddleware,
		outputAmount:         config.OutputAmount,
		keyFunc:              keyFunc,
		codec:                codec,
		states:               states,
		checkpoint:           ckpt,
		persistBatchSize:     config.PersistBatchSize,
		persistFlushInterval: config.PersistFlushInterval,
	}, nil
}

func (d *DistinctFilter[T]) Run() {
	defer d.close()
	if err := d.inputMiddleware.StartConsumingBatch(d.persistBatchSize, d.persistFlushInterval, func(msgs []middleware.Message, ack, nack func()) {
		d.handleBatch(msgs, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input middleware", "err", err)
	}
}

func (d *DistinctFilter[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	d.stopConsuming()
}

func (d *DistinctFilter[T]) stopConsuming() {
	if err := d.inputMiddleware.StopConsuming(); err != nil {
		slog.Error("While stopping input consumer", "err", err)
	}
}

func (d *DistinctFilter[T]) close() {
	cleanup.Close(d.inputMiddleware, d.outputMiddleware)
}

func (d *DistinctFilter[T]) handleBatch(msgs []middleware.Message, ack, nack func()) {
	modified := make(map[int]*clientState[T])
	completed := make(map[int]struct{})
	aborted := make(map[int]bool)

	for _, msg := range msgs {
		input, err := batch.Read(msg.Body, d.codec)
		if err != nil {
			slog.Error("decode failed", "err", err)
			continue
		}

		clientID := input.ClientID
		state := d.states.For(clientID)

		if input.Abort || aborted[clientID] {
			aborted[clientID] = true
			modified[clientID] = state
			continue
		}

		if state.tracker.IsDuplicate(int(input.SenderID), input.Seq) {
			slog.Warn("duplicate", "clientID", clientID, "senderID", input.SenderID, "seq", input.Seq)
			continue
		}

		if input.EOF {
			state.tracker.RegisterEOF(int(input.SenderID), 0, input.Seq)
		} else {
			state.tracker.RegisterBatch(int(input.SenderID))
			for i := range input.Records {
				rec := input.Records[i]
				key := d.keyFunc(rec)
				if _, ok := state.seen[key]; !ok {
					state.seen[key] = rec
				}
			}
		}

		state.tracker.Claim(int(input.SenderID), input.Seq)
		modified[clientID] = state

		if state.tracker.IsComplete(d.expectedEOFs) {
			if err := d.emit(clientID, state); err != nil {
				slog.Error("emit failed", "err", err)
				nack()
				d.stopConsuming()
				return
			}
			completed[clientID] = struct{}{}
		}
	}

	for clientID, state := range modified {
		if aborted[clientID] {
			if err := msgsend.SendAbort(d.outputMiddleware, middleware.BroadcastRoutingKey, clientID); err != nil {
				slog.Error("While emitting abort", "err", err)
				nack()
				d.stopConsuming()
				return
			}
			d.states.Delete(clientID)
			d.checkpoint.DeleteClient(clientID)
			continue
		}
		if _, done := completed[clientID]; done {
			d.states.Delete(clientID)
			d.checkpoint.DeleteClient(clientID)
			continue
		}
		if err := d.checkpoint.SaveClient(clientID, state); err != nil {
			slog.Error("persist failed, stopping", "err", err)
			nack()
			d.stopConsuming()
			return
		}
	}

	ack()
}

func (d *DistinctFilter[T]) emit(clientID int, state *clientState[T]) error {
	ot := outputtracker.New()

	keys := make([]string, 0, len(state.seen))
	for k := range state.seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	byShard := make(map[int][]T)
	for _, key := range keys {
		idx := shard.CalculateIndexForShard(clientID, key, d.outputAmount)
		byShard[idx] = append(byShard[idx], state.seen[key])
	}

	for idx, group := range byShard {
		rk := fmt.Sprintf("shard-%d", idx)
		seq := ot.RegisterBatch(rk)
		body := batch.Write(clientID, d.queryId, uint8(d.id), seq, group, d.codec)
		if err := d.outputMiddleware.Send(middleware.Message{Body: body, RoutingKey: rk}); err != nil {
			return err
		}
	}

	for i := range d.outputAmount {
		rk := fmt.Sprintf("shard-%d", i)
		total := ot.CountFor(rk)
		seq := ot.RegisterBatch(rk)
		eofBody := batch.WriteEOF(clientID, d.queryId, uint8(d.id), seq, uint32(total))
		if err := d.outputMiddleware.Send(middleware.Message{Body: eofBody, RoutingKey: rk}); err != nil {
			return err
		}
	}

	return nil
}
