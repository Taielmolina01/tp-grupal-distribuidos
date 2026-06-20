package acumaccounts

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type AcumAccounts struct {
	id int

	hasher shard.Hasher

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
