package join

import (
	"sync"
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type JoinType string

const (
	TransferAccountByBank JoinType = "transfer_account_by_bank" // Q2
)

type JoinConfig struct {
	Id      int
	MomHost string
	MomPort int
	QueryID uint8

	LeftInputQueue    string
	RightInputQueue   string
	OutputQueue       string
	LeftEofsExpected  int
	RightEofsExpected int

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type clientState struct {
	leftBuffer   map[string]transfer.TransferForQ2
	rightBuffer  map[string]account.Account
	leftTracker  *sendertracker.SenderTracker
	rightTracker *sendertracker.SenderTracker
	emitted      bool
}

type Join struct {
	id      int
	queryID uint8

	leftInput  newmiddleware.Middleware
	rightInput newmiddleware.Middleware
	output     middleware.Middleware

	leftEofsExpected  int
	rightEofsExpected int

	states     statemap.StateMap[clientState]
	checkpoint *checkpoint.Checkpoint[clientState]

	persistBatchSize     int
	persistFlushInterval time.Duration

	mu sync.Mutex
}
