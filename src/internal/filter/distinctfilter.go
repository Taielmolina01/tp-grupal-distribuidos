package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/worker"
)

// Inicializadores

func newDistinctFilter[T comparable, S comparable](
	config FilterConfig,
	compareFunc func(T, T) bool,
	keyFunc func(T) S,
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

	return &DistinctFilter[T, S]{
		id:            uint32(config.Id),
		inputQueue:    inputQueue,
		outputQueues:  outputQueues,
		alreadySeen:   map[int]map[S]bool{},
		compareFunc:   compareFunc,
		keyFunc:       keyFunc,
		shardCriteria: shardCriteria,
		codec:         codec,
		queryId:       config.QueryId,
	}, nil
}

func (distinctfilter *DistinctFilter[T, S]) Run() {
	slog.Info("Starting filter consumers", "filter_id", distinctfilter.id)
	if err := distinctfilter.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		distinctfilter.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T, S]) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()

	input, err := batch.Read([]byte(msg.Body), distinctfilter.codec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		eofBody := batch.WriteEOF(input.ClientID, distinctfilter.queryId, input.Total)
		for _, outputQueue := range distinctfilter.outputQueues {
			if err := outputQueue.Send(middleware.Message{Body: string(eofBody)}); err != nil {
				slog.Error("While broadcasting EOF to output queue", "err", err)
			}
		}
		return
	}

	seen, ok := distinctfilter.alreadySeen[input.ClientID]
	if !ok {
		seen = map[S]bool{}
		distinctfilter.alreadySeen[input.ClientID] = seen
	}

	// Keep only records not seen before, grouped by output shard.
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
		body := batch.Write(input.ClientID, distinctfilter.queryId, group, distinctfilter.codec)
		if err := distinctfilter.outputQueues[idx].Send(middleware.Message{Body: string(body)}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}
}

func (distinctfilter *DistinctFilter[T, S]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
