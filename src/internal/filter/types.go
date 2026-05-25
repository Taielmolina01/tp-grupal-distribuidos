package filter

import (
	"sync"
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
	BANK_DISTINCT           FilterType = "BANK_DISTINCT"
	AVERAGE_FILTER          FilterType = "AVERAGE_FILTER"
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
	Amount                float32
	StartDateRange        time.Time
	EndDateRange          time.Time
	Currencies            []string
	AmountTreshold        int
	FilterAmount          int
	OutputQueues          []string
	QueryId               uint8
	PaymentFormats        []string
}

type Filter[T comparable, O comparable] struct {
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
}

type FilterAndSplitter struct {
	id             uint32
	inputExchange  middleware.Middleware
	outputExchange middleware.Middleware
	filterFunction func(transfer.Transfer) bool
	splitFunction  func(transfer.Transfer) (transfer.SplittedTransfer, transfer.SplittedTransfer)
}

type DistinctFilter[T comparable, S comparable] struct {
	id            uint32
	inputQueue    middleware.Middleware
	outputQueues  []middleware.Middleware
	alreadySeen   map[int]map[S]bool
	compareFunc   func(T, T) bool
	keyFunc       func(T) S
	shardCriteria func(T) string
}

type AverageFilter struct {
	id                  uint32
	queryID             uint8
	inputTransfersQueue middleware.Middleware
	inputAvgsQueue      middleware.Middleware
	outputQueue         middleware.Middleware

	transfersRing    eofring.EofRingAlgorithm
	transfersMonitor msgmonitor.MessageMonitor
	transfersEofOut  middleware.Middleware

	avgsExpectedEofs int

	compareFunc func(float32, float32) bool

	state map[int]*avgFilterClientState
	lock  sync.Mutex
}

type avgFilterClientState struct {
	avgs                map[string]float32
	avgsReady           bool
	avgsEofsReceived    int
	bufferedTransfers   []transfer.Transfer
	transfersEofPending bool
	transfersEofRealAmt uint32
}

type ConvertedAmountFilter[T, S comparable] struct {
	leftInputQueue     middleware.Middleware
	rightInputQueue    middleware.Middleware
	outputQueue        middleware.Middleware
	compareFunc        func(t T, s S) bool
	queryId            uint8
	leftKeyFunc        func(S) string
	leftSecondKeyFunc  func(S) string
	leftValueFunc      func(S) float32
	rightKeyFunc       func(T) string
	rightsecondKeyFunc func(T) string
	rightValueFunc     func(T) float32
	conversionFunc     func(T, float32) S
	conversionsByDay   map[string]map[string]float32
	toSaveFunc         func(T, int) string
	fromSaveFunc       func(string) (T, int, error)
	eofRing            eofring.EofRingAlgorithm
	eofOutputQueue     middleware.Middleware
	handlerMessages    msgmonitor.MessageMonitor
	id                 uint32
}
