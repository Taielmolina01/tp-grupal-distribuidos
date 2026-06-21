package daterangesplitter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type DateRangeSplitterConfig struct {
	Id                int
	FilterCurrencyAmt int

	MomHost string
	MomPort int

	InputExchange    string
	InputQueue       string
	InputRoutingKeys []string

	AvgOutputExchange    string
	AvgOutputAmount      int
	FilterOutputExchange string
	FilterOutputAmount   int

	QueryID uint8

	AvgPeriodStart    time.Time
	AvgPeriodEnd      time.Time
	FilterPeriodStart time.Time
	FilterPeriodEnd   time.Time

	PersistPath          string
	PersistBatchSize     int
	PersistFlushInterval time.Duration
}

type DateRangeSplitter struct {
	id uint32

	inputExchange    newmiddleware.Middleware
	avgMiddleware    newmiddleware.Middleware
	filterMiddleware newmiddleware.Middleware

	avgOutputAmount    int
	filterOutputAmount int
	avgHasher          shard.Hasher
	filterHasher       shard.Hasher

	prevNodeAmt int
	queryID     uint8

	avgPeriodStart, avgPeriodEnd       time.Time
	filterPeriodStart, filterPeriodEnd time.Time

	states               statemap.StateMap[clientState]
	checkpoint           *checkpoint.Checkpoint[clientState]
	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
