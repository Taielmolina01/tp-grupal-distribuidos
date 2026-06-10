package filterandsplitter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/shard"
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
	eofInput         middleware.Middleware
	eofOutput        middleware.Middleware
	eofHandler       eofring.EofRingAlgorithm

	handlerMessages msgmonitor.MessageMonitor

	queryID uint8
}
