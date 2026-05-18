package join

import "tp-grupal-distribuidos/internal/common/middleware"

type JoinType string

const (
	TransferAccountByBank JoinType = "transfer_account_by_bank" // Q2
)

type JoinConfig struct {
	MomHost            string
	MomPort            int
	LeftInputExchange  string
	RightInputExchange string
	OutputExchange     string
	QueryID            uint8
}

type Join[L, R, O any] struct {
	leftInput   middleware.Middleware
	rightInput  middleware.Middleware
	output      middleware.Middleware
	leftBuffer  map[int]map[string]L
	rightBuffer map[int]map[string]R
	leftKey     func(L) string
	rightKey    func(R) string
	combine     func(L, R) O
	queryID     uint8
}
