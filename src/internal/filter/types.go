package filter

import (
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
)

type FilterType string

const (
	CURRENCY               FilterType = "CURRENCY"
	AMOUNT                 FilterType = "AMOUNT"
	DATE_RANGE             FilterType = "DATE_RANGE"
	DATE_RANGE_AND_PAYMENT FilterType = "DATE_RANGE_AND_PAYMENT"
)

type FilterConfig struct {
	Type           FilterType
	Id             int
	MomHost        string
	MomPort        int
	InputExchange  string
	OutputExchange string
	Amount         float32
	StartDateRange time.Time
	EndDateRange   time.Time
	Currencies     []string
}

type Filter[T comparable] struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	callback       func(T) bool
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
	alreadySeen    map[T]bool
	compareFunc    func(T, T) bool
}

type AverageFilter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	compareFunc    func(float32, float32) bool
}
