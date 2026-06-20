package filterandsplitter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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

	FilterCurrencyAmt int

	MomHost string
	MomPort int

	InputMiddlewareName  string
	InputMiddlewareQueue string
	InputShardKey        string

	QueryID     uint8
	PersistPath string
}

type FilterAndSplitter struct {
	id           int
	startDate    time.Time
	endDate      time.Time
	outputAmount int
	prevNodeAmt  int

	hasher shard.Hasher

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	queryID    uint8
	checkpoint *checkpoint.Checkpoint[clientState]

	states statemap.StateMap[clientState]
}

type clientState struct {
	transferTracker *sendertracker.SenderTracker
	outputTracker   *outputtracker.OutputTracker
}
