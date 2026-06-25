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
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	legacyConn := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	var (
		inputMiddleware newmiddleware.Middleware
		err             error
	)
	outputQueues := make([]middleware.Middleware, 0, len(config.OutputQueues))

	defer func() {
		if err != nil {
			if inputMiddleware != nil {
				if closeErr := inputMiddleware.Close(); closeErr != nil {
					slog.Error("While closing input middleware", "err", closeErr)
				}
			}
			for _, q := range outputQueues {
				if closeErr := q.Close(); closeErr != nil {
					slog.Error("While closing output queue", "err", closeErr)
				}
			}
		}
	}()

	inputMiddleware, err = newmiddleware.NewQueueMiddleware(connSettings, config.InputQueue)
	if err != nil {
		return nil, fmt.Errorf("creating input middleware: %w", err)
	}

	// TODO: esto tendríamos q meterlo en el archivo persist.go como hacemos ne los demas
	for _, name := range config.OutputQueues {
		var m middleware.Middleware
		m, err = middleware.CreateQueueMiddleware(name, legacyConn)
		if err != nil {
			return nil, fmt.Errorf("creating output queue %s: %w", name, err)
		}
		outputQueues = append(outputQueues, m)
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
		outputQueues:         outputQueues,
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
	if err := d.inputMiddleware.StartConsumingBatch(d.persistBatchSize, d.persistFlushInterval, func(msgs []newmiddleware.Message, ack, nack func()) {
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
	cleanup.Close(d.inputMiddleware)
	for _, q := range d.outputQueues {
		cleanup.Close(q)
	}
}

func (d *DistinctFilter[T]) handleBatch(msgs []newmiddleware.Message, ack, nack func()) {
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
			// TODO: Migrar a newgateway
			// if err := msgsend.SendAbort(d.outputMiddleware, newmiddleware.BroadcastRoutingKey, clientID); err != nil {
			// 	slog.Error("While emitting abort", "err", err)
			// 	nack()
			// 	d.stopConsuming()
			// 	return
			// }
			// d.states.Delete(clientID)
			// d.checkpoint.DeleteClient(clientID)
			// continue
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
		idx := shard.CalculateIndexForShard(clientID, key, len(d.outputQueues))
		byShard[idx] = append(byShard[idx], state.seen[key])
	}

	for idx, group := range byShard {
		seq := ot.RegisterBatch(strconv.Itoa(idx))
		body := batch.Write(clientID, d.queryId, uint8(d.id), seq, group, d.codec)
		if err := d.outputQueues[idx].Send(middleware.Message{Body: body}); err != nil {
			return err
		}
	}

	for idx, q := range d.outputQueues {
		rk := strconv.Itoa(idx)
		total := ot.CountFor(rk)
		seq := ot.RegisterBatch(rk)
		eofBody := batch.WriteEOF(clientID, d.queryId, uint8(d.id), seq, uint32(total))
		if err := q.Send(middleware.Message{Body: eofBody}); err != nil {
			return err
		}
	}

	return nil
}
