package aggregate

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type AggregateConfig struct {
	Id           int
	ExpectedEOFs int

	InputMiddlewarePrefix  string
	OutputMiddlewarePrefix string
	OutputAmount           int

	MaxBatchSize  int
	MaxBatchBytes int

	MomHost string
	MomPort int
	QueryID uint8

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type partial struct {
	totalSum   float64
	totalCount int
}

type AvgAggregator struct {
	id      int
	queryID uint8

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	prevNodeAmt   int
	outputAmount  int
	hasher        shard.Hasher
	maxBatchSize  int
	maxBatchBytes int

	states     statemap.StateMap[clientState]
	checkpoint *checkpoint.Checkpoint[clientState]

	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker      *sendertracker.SenderTracker
	acumuladores map[string]partial
}
