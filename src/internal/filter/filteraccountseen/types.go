package filteraccountseen

import (
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type FilterAccountSeenConfig struct {
	Id int

	ExpectedEOFs int

	OutputQueue string

	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	QueryID               int
	MaxBatchSize          int
	MaxBatchBytes         int

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration

	SenderTrackerCapacity int
}

type FilterAccountSeen struct {
	id int

	expectedEOFs  int
	maxBatchSize  int
	maxBatchBytes int

	inputMiddleware  middleware.Middleware
	outputMiddleware middleware.Middleware

	states statemap.StateMap[clientState]

	queryID int

	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker      *sendertracker.SenderTracker
	seenAccounts map[account.AccountIdentifier]struct{}
}
