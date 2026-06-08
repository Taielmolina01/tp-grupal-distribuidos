package filter

import (
	"time"

	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
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
	DATE_RANGE_AND_SPLITTER FilterType = "DATE_RANGE_AND_SPLITTER"
	AVERAGE_FILTER          FilterType = "AVERAGE_FILTER"
	COUNT_AND_FILTER        FilterType = "COUNT_AND_FILTER"
	TRANSFER_DISTINCT       FilterType = "TRANSFER_DISTINCT"
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

type Filter[T any, O any] struct {
	id              uint32
	inputExchange   middleware.Middleware
	outputExchange  middleware.Middleware
	filterFunction  func(T) bool
	eofHandler      eofring.EofRingAlgorithm
	handlerMessages msgmonitor.MessageMonitor
	outputQueueEof  middleware.Middleware
	filterType      FilterType
	outputTransform func(T) O
	queryId         uint8
	inputCodec      wire.Codec[T]
	outputCodec     wire.Codec[O]
}

type FilterAndSplitter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	filterFunction func(transfer.Transfer) bool
	splitFunction  func(transfer.Transfer) (transfer.SplittedTransfer, transfer.SplittedTransfer)
}

type ConvertedAmountFilter struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	queryId         uint8
	handlerMessages msgmonitor.MessageMonitor
	id              uint32
	quote           string
	amountThreshold float64
	pending         map[int][]transfer.FinalTransferForQ5
}
