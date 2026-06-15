package filteraccountseen

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
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
}

type FilterAccountSeen struct {
	id int

	mu sync.Mutex

	expectedEOFs  int
	maxBatchSize  int
	maxBatchBytes int

	inputMiddleware  newmiddleware.Middleware
	outputMiddleware newmiddleware.Middleware

	clientsState map[int]*clientState

	queryID int
}

type clientState struct {
	eofAmt       int
	seenAccounts map[account.AccountIdentifier]struct{}
	builder      *batch.Builder[queryresult.Query4Result]
	seqReceived  map[int]uint64
}

func (s *clientState) isDuplicateEOF(senderID int, seq uint64) bool {
	if seq == 0 {
		return false
	}
	if seq <= s.seqReceived[senderID] {
		return true
	}
	s.seqReceived[senderID] = seq
	return false
}
