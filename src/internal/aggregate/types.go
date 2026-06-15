package aggregate

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type AggregateConfig struct {
	Id              int
	AggregateAmount int
	SumAmount       int
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

type eofInfo struct {
	amount    int
	processed uint32
}

type AvgAggregator struct {
	id      int
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	msgMonitor   msgmonitor.ShardedMessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]partial
	eofsByClient map[int]eofInfo

	sumAmount int
}
