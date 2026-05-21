package join

import "tp-grupal-distribuidos/internal/common/middleware"

type JoinType string

const (
	TransferAccountByBank JoinType = "transfer_account_by_bank" // Q2
	SplittedTransfer      JoinType = "splitted_transfer"        // Q4
)

type JoinConfig struct {
	MomHost            string
	MomPort            int
	InputExchange      string
	LeftInputExchange  string
	RightInputExchange string
	OutputExchange     string
	QueryID            uint8
}

type Join[L, R, O any] struct {
	output middleware.Middleware
	leftBuffer  map[int]map[string]L //{clientID : {key : data}}
	rightBuffer map[int]map[string]R //{clientID : {key : data}}
	leftKey     func(L) string
	rightKey    func(R) string
	combine     func(L, R) O
	queryID     uint8
}
