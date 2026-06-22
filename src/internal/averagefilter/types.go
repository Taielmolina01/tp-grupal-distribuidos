package averagefilter

import (
	"os"
	"sync"

	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

type AverageFilterConfig struct {
	Id int

	MomHost string
	MomPort int

	InputTransfersMiddlewarePrefix string
	InputAvgsMiddlewarePrefix      string
	OutputQueue                    string

	ExpectedTransfersEofs int
	ExpectedAvgEofs       int

	QueryID uint8
}

type AverageFilter struct {
	id      uint32
	queryID uint8

	inputTransfersMiddleware newmiddleware.Middleware
	inputAvgsMiddleware      newmiddleware.Middleware
	outputQueue              middleware.Middleware

	transfersMonitor msgmonitor.MessageMonitor

	expectedTransfersEofs int
	avgsExpectedEofs      int

	lock  sync.Mutex
	state map[int]*clientState
}

type clientState struct {
	avgs               map[string]float64
	avgsReady          bool
	avgsEofsReceived   int
	expectedAvgRecords int

	transfersEofsReceived int
	transfersEofPending   bool

	pending     []queryresult.Query3Result
	bufferFiles map[string]*os.File
}
