package filterandsplitter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/middleware"
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

	FilterAndSpliterAmount int

	MomHost string
	MomPort int

	InputMiddlewareName  string
	InputMiddlewareQueue string
	InputRoutingKeys     []string

	QueryID uint8
}

type FilterAndSplitter struct {
	id        int
	startDate time.Time
	endDate   time.Time

	hasher shard.Hasher

	inputMiddleware  middleware.Middleware
	outputMiddleware newmiddleware.Middleware

	queryID uint8

	states statemap.StateMap[clientState]
}

type clientState struct {
	transferTracker *sendertracker.SenderTracker
	outputTracker   *outputtracker.OutputTracker
}
