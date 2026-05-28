package join

import (
	"sync"

	"tp-grupal-distribuidos/internal/common/middleware"
)

type JoinType string

const (
	TransferAccountByBank JoinType = "transfer_account_by_bank" // Q2
)

type JoinConfig struct {
	Id                int
	Amount            int
	MomHost           string
	MomPort           int
	LeftInputQueue    string
	RightInputQueue   string
	OutputQueue       string
	QueryID           uint8
	LeftEofsExpected  int
	RightEofsExpected int
}

type Join[L, R, O any] struct {
	output      middleware.Middleware
	leftBuffer  map[int]map[string]L //{clientID : {key : data}}
	rightBuffer map[int]map[string]R //{clientID : {key : data}}
	leftKey     func(L) string
	rightKey    func(R) string
	combine     func(L, R) O
	leftCombine func(L, L) L
	queryID     uint8
	mu          sync.Mutex
}
