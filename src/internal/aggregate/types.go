package aggregate

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type AggregateConfig struct {
	Id              int
	AggregateAmount int
	MomHost         string
	MomPort         int
	InputQueue      string
	OutputQueues    []string
	QueryID         uint8
}

type partial struct {
	totalSum   float64
	totalCount int
}

type AvgAggregator struct {
	id      int
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	eofInput     middleware.Middleware
	eofOutput    middleware.Middleware
	eofHandler   eofring.EofRingAlgorithm
	msgMonitor   msgmonitor.ShardedMessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]partial
}
