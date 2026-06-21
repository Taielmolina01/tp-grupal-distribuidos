package commonfilter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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
	shardKeys       func(O) []string
	inputCodec      wire.Codec[T]
	outputCodec     wire.Codec[O]

	inputExchange  newmiddleware.Middleware
	outputClusters []newmiddleware.ShardedCluster

	states               statemap.StateMap[clientState]
	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
