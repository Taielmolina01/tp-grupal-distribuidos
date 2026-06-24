package commondistinctfilter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type clientState[T any] struct {
	tracker *sendertracker.SenderTracker
	seen    map[string]T
}

type DistinctFilter[T comparable] struct {
	id           uint32
	queryId      uint8
	expectedEOFs int

	inputMiddleware newmiddleware.Middleware
	outputQueues    []middleware.Middleware

	keyFunc func(T) string
	codec   wire.Codec[T]

	states     statemap.StateMap[clientState[T]]
	checkpoint *checkpoint.Checkpoint[clientState[T]]

	persistBatchSize     int
	persistFlushInterval time.Duration
}
