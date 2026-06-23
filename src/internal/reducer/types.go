package reducer

import (
	"time"

	"tp-grupal-distribuidos/internal/common/checkpoint"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/statemap"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type ReducerType string

const (
	MAX_AMOUNT_FROM_BANK = "MAX_AMOUNT_FROM_BANK"
	COUNT                = "COUNT"
)

type ReducerConfig struct {
	Id          int
	MomHost     string
	MomPort     int
	QueryID     uint8
	ReducerType ReducerType

	InputMiddlewarePrefix string
	ExpectedEOFs          int
	OutputQueues          []string
	PersistPath           string
	PersistBatchSize      int
	PersistFlushInterval  time.Duration

	InputQueue        string
	InputEofsExpected int
}

type Reducer struct {
	id      int
	queryID uint8

	inputMiddleware newmiddleware.Middleware
	outputQueues    []middleware.Middleware

	prevNodeAmt int

	reducerFunction func(transfer.TransferAfterCurrency, transfer.TransferAfterCurrency) transfer.TransferAfterCurrency
	keyFunc         func(transfer.TransferAfterCurrency) string
	projectFunc     func(transfer.TransferAfterCurrency) transfer.TransferForQ2
	outputCodec     wire.Codec[transfer.TransferForQ2]

	states     statemap.StateMap[clientState]
	checkpoint *checkpoint.Checkpoint[clientState]

	persistBatchSize     int
	persistFlushInterval time.Duration
}

type clientState struct {
	tracker   *sendertracker.SenderTracker
	maxByBank map[string]transfer.TransferAfterCurrency
}

type CountReducer struct {
	countByClient     map[int]uint32
	eofsByClient      map[int]uint32
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	queryId           uint8
	inputEofsExpected uint32
}
