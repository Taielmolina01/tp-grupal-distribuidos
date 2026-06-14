package daterangesplitter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type DateRangeSplitterConfig struct {
	Id             int
	SplitterAmount int

	MomHost string
	MomPort int

	InputExchange    string
	InputQueue       string
	InputRoutingKeys []string

	OutputQueues []string

	QueryID uint8

	AvgPeriodStart    time.Time
	AvgPeriodEnd      time.Time
	FilterPeriodStart time.Time
	FilterPeriodEnd   time.Time
}

type DateRangeSplitter struct {
	id uint32

	inputExchange middleware.Middleware

	outputQueues []middleware.Middleware
	monitors     []msgmonitor.MessageMonitor
	eofInputs    []middleware.Middleware
	eofOutputs   []middleware.Middleware
	eofHandlers  []eofring.EofRingAlgorithm

	queryID uint8

	avgPeriodStart, avgPeriodEnd       time.Time
	filterPeriodStart, filterPeriodEnd time.Time
}
