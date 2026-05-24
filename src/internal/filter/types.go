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
	OutputQueues      []string
	AvgInputQueue     string
	AvgExpectedEofs   int
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
