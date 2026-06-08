package reducer

import (
	"tp-grupal-distribuidos/internal/common/middleware"
)

type ReducerConfig struct {
	MomHost           string
	MomPort           int
	QueryId           uint8
	InputQueue        string
	OutputQueues      []string
	InputEofsExpected int
}

type CountReducer struct {
	countByClient     map[int]uint32
	eofsByClient      map[int]uint32
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	queryId           uint8
	inputEofsExpected uint32
}
