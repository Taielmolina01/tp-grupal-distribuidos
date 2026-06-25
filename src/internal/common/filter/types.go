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

	OutputQueue    string
	OutputQueues   []string
	OutputClusters []shard.ClusterConfig

	Amount         float64
	StartDateRange time.Time
	EndDateRange   time.Time
	Currencies     []string
	FilterAmount   int
	QueryID        uint8
	PaymentFormats []string
	Quote          string

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
	IsLastNode           bool
}
