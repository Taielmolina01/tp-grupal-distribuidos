package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
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

	deserializedMsg, err := inner.DeserializeData[T](&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if deserializedMsg.IsEOF() {
		for _, outputQueue := range distinctfilter.outputQueues {
			if err := outputQueue.Send(msg); err != nil {
				slog.Error("While broadcasting EOF to output queue", "err", err)
			}
		}
		return
	}

	_, ok := distinctfilter.alreadySeen[deserializedMsg.ClientID]
	if !ok {
		distinctfilter.alreadySeen[deserializedMsg.ClientID] = map[S]bool{}
	}
	key := distinctfilter.keyFunc(deserializedMsg.Payload)
	if _, ok := distinctfilter.alreadySeen[deserializedMsg.ClientID][key]; !ok {
		if err := distinctfilter.outputQueues[shard.CalculateIndexForShard(
			deserializedMsg.ClientID,
			distinctfilter.shardCriteria(deserializedMsg.Payload),
			len(distinctfilter.outputQueues),
		)].Send(msg); err != nil {
			slog.Error("While sending message to output exchange", "err", err)
			return
		}
		distinctfilter.alreadySeen[deserializedMsg.ClientID][key] = true
	}
}

func (distinctfilter *DistinctFilter[T, S]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
