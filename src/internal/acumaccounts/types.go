package acumaccounts

import (
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type AcumAccountsConfig struct {
	Id int

	OutputMiddlewareAmount int
	OutputMiddlewarePrefix string

	MomHost string
	MomPort int

	ExpectedEOFs         int
	InputMiddlewarePrefix string

	QueryID int

	RequiredAmt int8

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type clientState struct {
	acum            map[account.AccountPair]int8
	transferTracker *sendertracker.SenderTracker
}

type AcumAccounts struct {
	id           int
	hasher       shard.Hasher
	outputAmount int

	expectedEOFs     int
	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	requiredAmt int8

	states  statemap.StateMap[clientState]
	queryID int

	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
}
