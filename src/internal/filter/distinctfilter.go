package filter

import (
	"log/slog"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := newmiddleware.NewQueueMiddleware(connSettings, config.InputQueue)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]newmiddleware.Middleware, len(config.OutputQueues))
	for i, routingKey := range config.OutputQueues {
		outputQueue, err := newmiddleware.NewQueueMiddleware(connSettings, routingKey)
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
	if err := distinctfilter.inputQueue.StartConsuming(func(msg newmiddleware.Message, ack, _ func()) {
		distinctfilter.handleMessage(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (distinctfilter *DistinctFilter[T, S]) handleMessage(msg newmiddleware.Message, ack func()) {
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

	clientSeen, ok := distinctfilter.alreadySeen[deserializedMsg.ClientID]
	if !ok {
		clientSeen = map[S]bool{}
		distinctfilter.alreadySeen[deserializedMsg.ClientID] = clientSeen
	}
	key := distinctfilter.keyFunc(deserializedMsg.Payload)
	if !clientSeen[key] {
		if err := distinctfilter.outputQueues[shard.CalculateIndexForShard(
			deserializedMsg.ClientID,
			distinctfilter.shardCriteria(deserializedMsg.Payload),
			len(distinctfilter.outputQueues),
		)].Send(msg); err != nil {
			slog.Error("While sending message to output exchange", "err", err)
			return
		}
		clientSeen[key] = true
	}
}

func (distinctfilter *DistinctFilter[T, S]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
