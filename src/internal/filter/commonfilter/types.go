package commonfilter

import (
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type Filter[T any, O any] struct {
	filterType      filter.FilterType
	id              uint32
	inputExchange   middleware.Middleware
	outputExchange  middleware.Middleware
	filterFunction  func(T) bool
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	outputQueueEof  middleware.Middleware
	outputTransform func(T) O
	queryId         uint8
	inputCodec      wire.Codec[T]
	outputCodec     wire.Codec[O]
}
