package reducer

import (
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type ReducerType string

const (
	MAX_AMOUNT_FROM_BANK = "MAX_AMOUNT_FROM_BANK"
	COUNT                = "COUNT"
)

type ReducerConfig struct {
	Id                int
	ReducerAmount     int
	MomHost           string
	MomPort           int
	InputExchange     string
	QueryId           uint8
	InputQueue        string
	OutputQueues      []string
	InputRoutingKeys  []string
	InputEofsExpected int
	JoinsAmount       int
	ReducerType       ReducerType
}

type Reducer[T comparable] struct {
	id                int
	inputExchange     newmiddleware.Middleware
	outputQueues      []newmiddleware.Middleware
	reducerMonitor    ReducerMonitor[T]
	reducerFunction   func(T, T) T
	keyFunc           func(T) string
	eofHandler        eofring.EofRingAlgorithm
	handlerMessages   msgmonitor.MessageMonitor
	outputQueueEof    newmiddleware.Middleware
	queryId           uint8
	inputEofsExpected int
	inputEofCount     map[int]int
	totalRealAmount   map[int]uint32
}

type CountReducer struct {
	countByClient     map[int]uint32
	eofsByClient      map[int]uint32
	inputQueue        newmiddleware.Middleware
	outputQueue       newmiddleware.Middleware
	queryId           uint8
	inputEofsExpected uint32
}
