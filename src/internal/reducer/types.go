package reducer

import (
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
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
}

type Reducer[T comparable] struct {
	id                int
	inputExchange     middleware.Middleware
	outputQueues      []middleware.Middleware
	reducerMonitor    ReducerMonitor[T]
	reducerFunction   func(T, T) T
	keyFunc           func(T) string
	eofHandler        eofring.EofRingAlgorithm
	handlerMessages   msgmonitor.MessageMonitor
	outputQueueEof    middleware.Middleware
	queryId           uint8
	inputEofsExpected int
	inputEofCount     map[int]int
	totalRealAmount   map[int]uint32
}
