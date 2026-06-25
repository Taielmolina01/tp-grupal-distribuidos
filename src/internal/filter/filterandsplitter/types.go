package filterandsplitter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type FilterAndSplitterConfig struct {
	Id        int
	StartDate time.Time
	EndDate   time.Time

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	ExpectedEOFs int

	MomHost string
	MomPort int

	InputMiddlewarePrefix string

	QueryID     uint8
	PersistPath string

	PersistBatchSize     int
	PersistFlushInterval time.Duration

	SenderTrackerCapacity int
}

type FilterAndSplitter struct {
	id           int
	startDate    time.Time
	endDate      time.Time
	outputAmount int
	prevNodeAmt  int

	hasher shard.Hasher

	inputMiddleware  middleware.Middleware
	outputMiddleware middleware.Middleware

	queryID    uint8
	checkpoint *checkpoint.Checkpoint[clientState]

	states statemap.StateMap[clientState]

	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
