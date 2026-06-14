package averagefilter

import (
	"os"
	"sync"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

type AverageFilterConfig struct {
	Id           int
	FilterAmount int

	MomHost string
	MomPort int

	InputTransfersQueue string
	InputAvgsQueue      string
	OutputQueue         string

	AvgsExpectedEofs int

	QueryID uint8
}

type AverageFilter struct {
	id      uint32
	queryID uint8

	inputTransfersQueue middleware.Middleware
	inputAvgsQueue      middleware.Middleware
	outputQueue         middleware.Middleware

	transfersEofOut  middleware.Middleware
	transfersEofIn   middleware.Middleware
	transfersRing    eofring.EofRingAlgorithm
	transfersMonitor msgmonitor.MessageMonitor

	avgsExpectedEofs int

	lock  sync.Mutex
	state map[int]*clientState
}

type clientState struct {
	avgs                map[string]float64
	avgsReady           bool
	ringeof             bool
	avgsEofsReceived    int
	expectedAvgRecords  int
	transfersEofPending bool
	transfersEofRealAmt uint32
	pending             []queryresult.Query3Result
	bufferFiles         map[string]*os.File
}
