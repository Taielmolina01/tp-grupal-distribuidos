package filter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterType string

const (
	CURRENCY                FilterType = "CURRENCY"
	AMOUNT                  FilterType = "AMOUNT"
	DATE_RANGE              FilterType = "DATE_RANGE"
	DATE_RANGE_AND_PAYMENT  FilterType = "DATE_RANGE_AND_PAYMENT"
	COUNT_AND_FILTER        FilterType = "COUNT_AND_FILTER"
	DATE_RANGE_AND_SPLITTER FilterType = "DATE_RANGE_AND_SPLITTER"
	TRANSFER_DISTINCT       FilterType = "TRANSFER_DISTINCT"
	ACCOUNT_DISTINCT        FilterType = "ACCOUNT_DISTINCT"
	AVERAGE_FILTER          FilterType = "AVERAGE_FILTER"
)

type FilterConfig struct {
	Type              FilterType
	Id                int
	MomHost           string
	MomPort           int
	InputExchange     string
	InputQueue        string
	InputRoutingKeys  []string
	OutputExchange    string
	OutputQueue       string
	OutputRoutingKeys []string
	Amount            float32
	StartDateRange    time.Time
	EndDateRange      time.Time
	Currencies        []string
	AmountTreshold    int
	FilterAmount      int
}

type Filter[T comparable, O comparable] struct {
	id              uint32
	inputExchange   middleware.Middleware
	outputExchange  middleware.Middleware
	callback        func(T) bool
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	outputQueueEof  middleware.Middleware
	filterType      FilterType
	outputTransform func(T) O
	queryId         int
}

type FilterAndSplitter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	filterFunction func(transfer.Transfer) bool
	splitFunction  func(transfer.Transfer) (transfer.SplittedTransfer, transfer.SplittedTransfer)
}

type DistinctFilter[T comparable] struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	// alreadySeen    map[T]bool
	compareFunc func(T, T) bool
}

type AverageFilter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	compareFunc    func(float32, float32) bool
}
