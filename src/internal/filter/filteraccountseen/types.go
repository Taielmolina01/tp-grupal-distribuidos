package filteraccountseen

import (
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type FilterAccountSeenConfig struct {
	Id int

	ExpectedEOFs int

	OutputMiddleware string

	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	QueryID               int
	MaxBatchSize          int
	MaxBatchBytes         int

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type FilterAccountSeen struct {
	id int

	expectedEOFs  int
	maxBatchSize  int
	maxBatchBytes int

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	states statemap.StateMap[clientState]

	queryID int

	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker      *sendertracker.SenderTracker
	seenAccounts map[account.AccountIdentifier]struct{}
	builder      *batch.Builder[queryresult.Query4Result]
}
