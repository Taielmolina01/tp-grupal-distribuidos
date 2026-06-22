package aggregate

import (
	"sync"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type AggregateConfig struct {
	Id           int
	ExpectedEOFs int

	InputMiddlewarePrefix string
	OutputQueues          []string

	MomHost string
	MomPort int
	QueryID uint8
}

type partial struct {
	totalSum   float64
	totalCount int
}

type eofInfo struct {
	amount    int
	processed uint32
}

type AvgAggregator struct {
	id      int
	queryID uint8

	inputMiddleware newmiddleware.Middleware
	outputQueues    []middleware.Middleware
	msgMonitor      msgmonitor.ShardedMessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]partial
	eofsByClient map[int]eofInfo

	expectedEOFs int
}
