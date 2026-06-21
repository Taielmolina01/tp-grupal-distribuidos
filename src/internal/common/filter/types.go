package filter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/shard"
)

type FilterConfig struct {
	Id      int
	MomHost string
	MomPort int

	InputMiddlewarePrefix string
	InputQueue            string

	OutputQueue       string
	OutputRoutingKeys []string
	OutputClusters    []shard.ClusterConfig

	LeftInputQueue        string
	RightInputQueue       string
	RightInputExchange    string
	RightInputRoutingKeys []string

	Amount         float64
	StartDateRange time.Time
	EndDateRange   time.Time
	Currencies     []string
	AmountTreshold int
	FilterAmount   int
	OutputQueues   []string
	QueryID        uint8
	PaymentFormats []string
	Quote          string

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}
