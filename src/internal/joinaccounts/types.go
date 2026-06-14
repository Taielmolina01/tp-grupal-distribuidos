package joinaccounts

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/channels/qualifiedaccount"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/shard"
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
}

type clientState struct {
	left  map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}
	right map[account.AccountIdentifier]map[account.AccountIdentifier]struct{}

	qualifyingLeft  map[account.AccountIdentifier]struct{}
	qualifyingRight map[account.AccountIdentifier]struct{}

	qualifiedBatch *batch.Builder[qualifiedaccount.QualifiedAccount]

	transferEOFReceived bool
	transferEOFTotal    uint32
	qualifiedEOFCount   int

	seqSent              uint64
	transferSeqReceived  map[int]uint64
	qualifiedSeqReceived map[int]uint64
}

func (s *clientState) nextSeq() uint64 {
	s.seqSent++
	return s.seqSent
}

func (s *clientState) isDuplicateTransfer(senderID int, seq uint64) bool {
	if seq == 0 {
		return false
	}
	if seq <= s.transferSeqReceived[senderID] {
		return true
	}
	s.transferSeqReceived[senderID] = seq
	return false
}

func (s *clientState) isDuplicateQualified(senderID int, seq uint64) bool {
	if seq == 0 {
		return false
	}
	if seq <= s.qualifiedSeqReceived[senderID] {
		return true
	}
	s.qualifiedSeqReceived[senderID] = seq
	return false
}

type JoinAccounts struct {
	id int

	hasher shard.Hasher

	inputMiddleware           newmiddleware.Middleware
	qualifiedInputMiddleware  newmiddleware.Middleware
	qualifiedOutputMiddleware newmiddleware.Middleware
	outputMiddleware          newmiddleware.Middleware

	peerAmount    int
	threshold     int
	maxBatchSize  int
	maxBatchBytes int

	mu           sync.Mutex
	clientsState map[int]*clientState
	queryID      int
}
