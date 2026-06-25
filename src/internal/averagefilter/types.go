package averagefilter

import (
	"sync"
	"time"

	"tp-grupal-distribuidos/internal/common/appendlog"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type AverageFilterConfig struct {
	Id int

	MomHost string
	MomPort int

	InputTransfersMiddlewarePrefix string
	InputAvgsMiddlewarePrefix      string
	OutputQueue                    string

	ExpectedTransfersEofs int
	ExpectedAvgEofs       int

	MaxBatchSize  int
	MaxBatchBytes int

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration

	QueryID uint8
}

type AverageFilter struct {
	id      uint32
	queryID uint8

	inputTransfersMiddleware middleware.Middleware
	inputAvgsMiddleware      middleware.Middleware
	outputQueue              middleware.Middleware

	expectedTransfersEofs int
	avgsExpectedEofs      int

	maxBatchSize  int
	maxBatchBytes int

	states     statemap.StateMap[clientState]
	checkpoint *checkpoint.Checkpoint[clientState]

	persistBatchSize     int
	persistFlushInterval time.Duration
	transferLogDir       string
	transferLogs         map[int]*appendlog.Log[transfer.TransferForQ3Filter]

	lock sync.Mutex
}

type clientState struct {
	avgs map[string]float64

	transfersTracker *sendertracker.SenderTracker
	avgsTracker      *sendertracker.SenderTracker
}
