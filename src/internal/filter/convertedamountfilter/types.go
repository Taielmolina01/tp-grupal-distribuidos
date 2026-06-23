package convertedamountfilter

import (
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type ConvertedAmountFilter struct {
	InputQueue      newmiddleware.Middleware
	OutputQueue     middleware.Middleware
	QueryID         uint8
	HandlerMessages msgmonitor.MessageMonitor
	Id              uint32
	Quote           string
	AmountThreshold float64
	Pending         map[int][]transfer.FinalTransferForQ5
}
