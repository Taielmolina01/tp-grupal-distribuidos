package commondistinctfilter

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/worker"
)

const FILTER_STATE_FILE = "common_distinct_filter_%d"

// Inicializadores

func NewDistinctFilter[T comparable](
	config filter.FilterConfig,
	compareFunc func(T, T) bool,
	keyFunc func(T) string,
	shardCriteria func(T) string,
	codec wire.Codec[T],
) (worker.Worker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)

	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, len(config.OutputQueues))
	for i, routingKey := range config.OutputQueues {
		outputQueue, err := middleware.CreateQueueMiddleware(routingKey, connSettings)
		if err != nil {
			return nil, err
		}
		outputQueues[i] = outputQueue
	}

	distinctfilter := &DistinctFilter[T]{
		id:            uint32(config.Id),
		inputQueue:    inputQueue,
		outputQueues:  outputQueues,
		alreadySeen:   map[int]map[string]bool{},
		compareFunc:   compareFunc,
		keyFunc:       keyFunc,
		stateFilePath: fmt.Sprintf(FILTER_STATE_FILE, config.Id),
		shardCriteria: shardCriteria,
		codec:         codec,
		queryId:       config.QueryId,
	}

	distinctfilter.LoadState()

	return distinctfilter, nil
}

func (distinctfilter *DistinctFilter[T]) Run() {
	defer distinctfilter.close()
	slog.Info("Starting filter consumers", "filter_id", distinctfilter.id)
	if err := distinctfilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		distinctfilter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()

	input, err := batch.Read(msg.Body, distinctfilter.codec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		eofBody := batch.WriteEOF(input.ClientID, distinctfilter.queryId, 0, 0, input.Total)
		for _, outputQueue := range distinctfilter.outputQueues {
			if err := outputQueue.Send(middleware.Message{Body: eofBody}); err != nil {
				slog.Error("While broadcasting EOF to output queue", "err", err)
			}
		}
		if err := distinctfilter.SaveState(); err != nil {
			slog.Error("While saving state", "err", err)
		}
		return
	}

	seen, ok := distinctfilter.alreadySeen[input.ClientID]
	if !ok {
		seen = map[string]bool{}
		distinctfilter.alreadySeen[input.ClientID] = seen
	}

	byShard := make(map[int][]T)
	for i := range input.Records {
		record := input.Records[i]
		key := distinctfilter.keyFunc(record)
		if seen[key] {
			continue
		}
		seen[key] = true
		idx := shard.CalculateIndexForShard(
			input.ClientID,
			distinctfilter.shardCriteria(record),
			len(distinctfilter.outputQueues),
		)
		byShard[idx] = append(byShard[idx], record)
	}

	for idx, group := range byShard {
		body := batch.Write(input.ClientID, distinctfilter.queryId, 0, 0, group, distinctfilter.codec)
		if err := distinctfilter.outputQueues[idx].Send(middleware.Message{Body: body}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}

	if err := distinctfilter.SaveState(); err != nil {
		slog.Error("While saving state", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T]) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")

	if err := distinctfilter.inputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping consumer", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T]) close() {
	if err := distinctfilter.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	for _, outputQueue := range distinctfilter.outputQueues {
		if err := outputQueue.Close(); err != nil {
			slog.Error("While closing output queue", "err", err)
		}
	}
}

// State persistence

func (distinctfilter *DistinctFilter[T]) SaveState() error {
	w := wire.NewWriter()

	w.Uint32(uint32(len(distinctfilter.alreadySeen)))
	for clientId, seen := range distinctfilter.alreadySeen {
		w.Uint32(uint32(clientId))
		w.Uint32(uint32(len(seen)))
		for key := range seen {
			w.String(key)
		}
	}
	tmp := distinctfilter.stateFilePath + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, distinctfilter.stateFilePath)
}

func (distinctfilter *DistinctFilter[T]) LoadState() error {
	state, err := os.ReadFile(distinctfilter.stateFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	r := wire.NewReader(state)

	clientsCount := r.Uint32()
	for i := uint32(0); i < clientsCount; i++ {
		clientId := r.Uint32()
		keysCount := r.Uint32()
		seen := map[string]bool{}
		for j := uint32(0); j < keysCount; j++ {
			key := r.String()
			seen[key] = true
		}
		distinctfilter.alreadySeen[int(clientId)] = seen
	}

	return nil
}
