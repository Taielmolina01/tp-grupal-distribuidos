package msgmonitor

import (
	"sync"
)

type MessageMonitor interface {
	GetProccessedMessagesAmountByClientId(int) uint32
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

func (monitor *messageMonitorImpl) GetProccessedMessagesAmountByClientId(clientID int) uint32 {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()
	amount, _ := monitor.processedMessagesByClient[clientID]
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
