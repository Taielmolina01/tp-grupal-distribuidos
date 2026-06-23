package commondistinctfilter

import (
	"log/slog"
	"strconv"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/worker"
)

// Inicializadores

func NewDistinctFilter[T comparable, S comparable](
	config filter.FilterConfig,
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
		id:             uint32(config.Id),
		inputQueue:     inputQueue,
		outputQueues:   outputQueues,
		alreadySeen:    map[int]map[S]bool{},
		outputTrackers: map[int]*outputtracker.OutputTracker{},
		compareFunc:    compareFunc,
		keyFunc:        keyFunc,
		shardCriteria:  shardCriteria,
		codec:          codec,
		queryId:        config.QueryID,
	}, nil
}

func (distinctfilter *DistinctFilter[T, S]) trackerFor(clientID int) *outputtracker.OutputTracker {
	ot, ok := distinctfilter.outputTrackers[clientID]
	if !ok {
		ot = outputtracker.New()
		distinctfilter.outputTrackers[clientID] = ot
	}
	return ot
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

	input, err := batch.Read(msg.Body, distinctfilter.codec)
	if err != nil {
		slog.Error("While deserializing input batch", "err", err)
		return
	}

	if input.EOF {
		ot := distinctfilter.trackerFor(input.ClientID)
		for idx, outputQueue := range distinctfilter.outputQueues {
			rk := strconv.Itoa(idx)
			total := ot.CountFor(rk)
			seq := ot.RegisterBatch(rk)
			eofBody := batch.WriteEOF(input.ClientID, distinctfilter.queryId, uint8(distinctfilter.id), seq, uint32(total))
			if err := outputQueue.Send(middleware.Message{Body: eofBody}); err != nil {
				slog.Error("While broadcasting EOF to output queue", "err", err)
			}
		}
		delete(distinctfilter.outputTrackers, input.ClientID)
		delete(distinctfilter.alreadySeen, input.ClientID)
		return
	}

	seen, ok := distinctfilter.alreadySeen[input.ClientID]
	if !ok {
		seen = map[S]bool{}
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

	ot := distinctfilter.trackerFor(input.ClientID)
	for idx, group := range byShard {
		seq := ot.RegisterBatch(strconv.Itoa(idx))
		body := batch.Write(input.ClientID, distinctfilter.queryId, uint8(distinctfilter.id), seq, group, distinctfilter.codec)
		if err := distinctfilter.outputQueues[idx].Send(middleware.Message{Body: body}); err != nil {
			slog.Error("While sending output batch", "err", err)
		}
	}
}

func (distinctfilter *DistinctFilter[T, S]) HandleSignals() {

}

// Handler para la working queue que comparten las distintas intancias de sum.
