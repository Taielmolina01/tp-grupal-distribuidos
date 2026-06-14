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

type AvgAggregator struct {
	id      int
	queryID uint8

	inputQueue    middleware.Middleware
	outputQueues  []middleware.Middleware
	stateFilePath string

	msgMonitor msgmonitor.ShardedMessageMonitor

	mu        sync.Mutex
	accums    map[int]map[string]partial
	eofCounts map[int]int
	eofTotals map[int]uint32

	sumAmount  int
	stateSaver StateSaver
}
