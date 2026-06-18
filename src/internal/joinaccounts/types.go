package joinaccounts

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
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

	InputMiddlewareAmt uint8 //AGREGAR LA CFG DE ESTO ANTES D COLGARME jejox
}

type clientState struct {
	left  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}
	right map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}

	qualifyingLeft  map[account.AccountIdentifier]struct{}
	qualifyingRight map[account.AccountIdentifier]struct{}

	qualifiedBatch *batch.Builder[qualifiedaccount.QualifiedAccount]

	transferTracker  *sendertracker.SenderTracker
	qualifiedTracker *sendertracker.SenderTracker
	transfersDone    bool
}

type JoinAccounts struct {
	id int

	hasher shard.Hasher

	inputMiddleware           newmiddleware.Middleware
	inputMiddlewareAmt        uint8
	qualifiedInputMiddleware  newmiddleware.Middleware
	qualifiedOutputMiddleware newmiddleware.Middleware
	outputMiddleware          newmiddleware.Middleware

	peerAmount    int
	threshold     int
	maxBatchSize  int
	maxBatchBytes int

	states statemap.StateMap[clientState]

	mu      sync.Mutex
	queryID int
}
