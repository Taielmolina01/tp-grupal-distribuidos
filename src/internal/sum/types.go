package sum

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/eofring"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type SumConfig struct {
	Id           int
	SumAmount    int
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQueues []string
	QueryID      uint8
}

type SumByPaymentFormat struct {
	id      int
	queryID uint8

	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	eofInput     middleware.Middleware
	eofOutput    middleware.Middleware
	eofHandler   eofring.EofRingAlgorithm
	msgMonitor   msgmonitor.MessageMonitor

	mu           sync.Mutex
	acumuladores map[int]map[string]transfer.SumByMethod
}
