package joinaccounts

import (
	"sync"
	"time"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type JoinAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	QualifiedExchange     string
	PeerAmount            int

	Threshold     int
	QueryID       int
	MaxBatchSize  int
	MaxBatchBytes int
	ExpectedEOFs  int

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type transferPartialState struct {
	transferTracker *sendertracker.SenderTracker
	left            map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}
	right           map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}
}

type qualifiedPartialState struct {
	qualifiedTracker *sendertracker.SenderTracker
	qualifyingLeft   map[account.AccountIdentifier]struct{}
	qualifyingRight  map[account.AccountIdentifier]struct{}
}

type clientState struct {
	left  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}
	right map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}

	qualifyingLeft  map[account.AccountIdentifier]struct{}
	qualifyingRight map[account.AccountIdentifier]struct{}

	pendingQualified []qualifiedaccount.QualifiedAccount

	transferTracker  *sendertracker.SenderTracker
	qualifiedTracker *sendertracker.SenderTracker
}

type JoinAccounts struct {
	id int

	hasher       shard.Hasher
	outputAmount int

	inputMiddleware           middleware.Middleware
	expectedEOFs              int
	qualifiedInputMiddleware  middleware.Middleware
	qualifiedOutputMiddleware middleware.Middleware
	outputMiddleware          middleware.Middleware

	peerAmount    int
	threshold     int
	maxBatchSize  int
	maxBatchBytes int

	states statemap.StateMap[clientState]

	transferCheckpoint  *checkpoint.Checkpoint[transferPartialState]
	qualifiedCheckpoint *checkpoint.Checkpoint[qualifiedPartialState]

	persistBatchSize     int
	persistFlushInterval time.Duration

	mu      sync.Mutex
	queryID int
}
