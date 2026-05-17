package msgmonitor

import (
	"sync"
)

type MessageMonitor interface {
	GetProcessedMessagesAmountByClientId(int) uint32
	AddProcessedMessagesAmountByClientId(int, uint32)
	Close()
}

type messageMonitorImpl struct {
	processedMessagesByClient map[int]uint32
	processedMessagesMutex    sync.Mutex
}

func NewMessageMonitor() MessageMonitor {
	return &messageMonitorImpl{
		processedMessagesByClient: map[int]uint32{},
		processedMessagesMutex:    sync.Mutex{},
	}
}

func (monitor *messageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()
	amount := monitor.processedMessagesByClient[clientID]
	return amount
}

func (monitor *messageMonitorImpl) AddProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()
	monitor.processedMessagesByClient[clientID] += amount
}

func (monitor *messageMonitorImpl) Close() {
	clear(monitor.processedMessagesByClient)
}
