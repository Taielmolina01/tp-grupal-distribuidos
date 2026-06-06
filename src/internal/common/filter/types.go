package filter

import (
	"time"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type FilterType string

const (
	CURRENCY                FilterType = "CURRENCY"
	AMOUNT                  FilterType = "AMOUNT"
	DATE_RANGE              FilterType = "DATE_RANGE"
	DATE_RANGE_AND_PAYMENT  FilterType = "DATE_RANGE_AND_PAYMENT"
	DATE_RANGE_AND_SPLITTER FilterType = "DATE_RANGE_AND_SPLITTER"
	AVERAGE_FILTER          FilterType = "AVERAGE_FILTER"
	COUNT_AND_FILTER        FilterType = "COUNT_AND_FILTER"
	TRANSFER_DISTINCT       FilterType = "TRANSFER_DISTINCT"
	BANK_DISTINCT           FilterType = "BANK_DISTINCT"
	CONVERTED_AMOUNT_FILTER FilterType = "CONVERTED_AMOUNT_FILTER"
)

type FilterConfig struct {
	Type                  FilterType
	Id                    int
	MomHost               string
	MomPort               int
	InputExchange         string
	InputQueue            string
	InputRoutingKeys      []string
	OutputExchange        string
	OutputQueue           string
	OutputRoutingKeys     []string
	LeftInputQueue        string
	RightInputQueue       string
	RightInputExchange    string
	RightInputRoutingKeys []string
	Amount                float64
	StartDateRange        time.Time
	EndDateRange          time.Time
	Currencies            []string
	AmountTreshold        int
	FilterAmount          int
	OutputQueues          []string
	QueryId               uint8
	PaymentFormats        []string
	Quote                 string
}

type FilterAndSplitter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	filterFunction func(transfer.Transfer) bool
	splitFunction  func(transfer.Transfer) (transfer.SplittedTransfer, transfer.SplittedTransfer)
}
