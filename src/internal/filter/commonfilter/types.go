package commonfilter

import (
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type Filter[T any, O any] struct {
	id          uint32
	queryId     uint8
	filterType  filter.FilterType
	inputAmount int

	filterFunction  func(T) bool
	outputTransform func(T) O
	shardKeys       func(O) []string
	inputCodec      wire.Codec[T]
	outputCodec     wire.Codec[O]

	inputExchange  newmiddleware.Middleware
	outputExchange newmiddleware.Middleware
	multiHasher    shard.MultiClusterHasher

	states statemap.StateMap[clientState]
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
