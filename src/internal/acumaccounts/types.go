package acumaccounts

import (
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/shard"
)

type AcumAccounts struct {
	id int

	hasher shard.Hasher

	expectedEOFs     int
	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	requiredAmt int8

	clientsState map[int]*clientState
	queryID      int
}
