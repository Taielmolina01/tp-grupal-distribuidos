package commondistinctfilter

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
)

type DistinctFilter[T comparable, S comparable] struct {
	id             uint32
	inputQueue     middleware.Middleware
	outputQueues   []middleware.Middleware
	alreadySeen    map[int]map[S]bool
	outputTrackers map[int]*outputtracker.OutputTracker
	compareFunc    func(T, T) bool
	keyFunc        func(T) S
	shardCriteria  func(T) string
	codec          wire.Codec[T]
	queryId        uint8
}
