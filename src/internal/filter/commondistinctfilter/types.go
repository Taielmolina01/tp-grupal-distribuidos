package commondistinctfilter

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
)

type DistinctFilter[T comparable] struct {
	id            uint32
	inputQueue    middleware.Middleware
	outputQueues  []middleware.Middleware
	alreadySeen   map[int]map[string]bool
	compareFunc   func(T, T) bool
	keyFunc       func(T) string
	shardCriteria func(T) string
	codec         wire.Codec[T]
	queryId       uint8
	stateFilePath string
}
