package commonfilter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type Filter[T any, O any] struct {
	id          uint32
	queryId     uint8
	inputAmount int

	filterFunction  func(T) bool
	outputTransform func(T) O
	shardKeys       func(O, uint64) []string
	inputCodec      wire.Codec[T]
	outputCodec     wire.Codec[O]

	inputExchange  middleware.Middleware
	outputClusters []middleware.ShardedCluster

	states               statemap.StateMap[clientState]
	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
	isLastNode           bool
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
