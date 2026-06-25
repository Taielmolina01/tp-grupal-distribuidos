package fetcher

import (
	"time"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/priorityqueue"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type FetcherConfig struct {
	MomHost                string
	MomPort                int
	InputMiddlewarePrefix  string
	Id                     int
	OutputMiddlewarePrefix string
	ExpectedInputSenders   int
	Quote                  string
	QueryID                uint8
	PersistPath            string
	OutputAmount           int

	SenderTrackerCapacity int
}

type Fetcher struct {
	inputQueue       middleware.Middleware
	outputMiddleware middleware.Middleware
	queryId          uint8
	quote            string
	ratesCache       map[string]heapDTO
	states           statemap.StateMap[clientState]
	expectedSenders  int
	ratesCacheHeap   priorityqueue.PriorityQueue[heapDTO]
	checkpoint       *checkpoint.Checkpoint[clientState]
	outputAmount     int
	hasher           shard.Hasher
}

type heapDTO struct {
	time               time.Time
	apiResponseRateVal *apiResponseRate
}

type apiResponseRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
