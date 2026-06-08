package msgmonitor

import (
	"sync"
)

type MessageMonitor interface {
	GetProcessedMessagesAmountByClientId(int) uint32
	AddProcessedMessagesAmountByClientId(int, uint32)
	GetForwardedMessagesAmountByClientId(int) uint32
	AddForwardedMessagesAmountByClientId(int, uint32)
	RemoveClient(int)
	Close()
}

type messageMonitorImpl struct {
	processedByClient      map[int]uint32
	forwardedByClient      map[int]uint32
	processedMessagesMutex sync.Mutex
	forwardedMessagesMutex sync.Mutex
}

func NewMessageMonitor() MessageMonitor {
	return &messageMonitorImpl{
		processedByClient:      map[int]uint32{},
		forwardedByClient:      map[int]uint32{},
		processedMessagesMutex: sync.Mutex{},
		forwardedMessagesMutex: sync.Mutex{},
	}
}

func (monitor *messageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	amount := monitor.processedByClient[clientID]
	return amount
}

func (monitor *messageMonitorImpl) AddProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	actual := monitor.processedByClient[clientID]
	actual += amount
	monitor.processedByClient[clientID] = actual
}

func (monitor *messageMonitorImpl) GetForwardedMessagesAmountByClientId(clientID int) uint32 {
	monitor.forwardedMessagesMutex.Lock()
	defer monitor.forwardedMessagesMutex.Unlock()

	amount := monitor.forwardedByClient[clientID]
	return amount
}

func (monitor *messageMonitorImpl) AddForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.forwardedMessagesMutex.Lock()
	defer monitor.forwardedMessagesMutex.Unlock()

	actual := monitor.forwardedByClient[clientID]
	actual += amount
	monitor.forwardedByClient[clientID] = actual
}

func (monitor *messageMonitorImpl) RemoveClient(clientID int) {
	monitor.processedMessagesMutex.Lock()
	delete(monitor.processedByClient, clientID)
	monitor.processedMessagesMutex.Unlock()

	monitor.forwardedMessagesMutex.Lock()
	delete(monitor.forwardedByClient, clientID)
	defer monitor.forwardedMessagesMutex.Unlock()
}

func (monitor *messageMonitorImpl) Close() {
	monitor.processedMessagesMutex.Lock()
	clear(monitor.processedByClient)
	monitor.processedMessagesMutex.Unlock()

	monitor.forwardedMessagesMutex.Lock()
	clear(monitor.forwardedByClient)
	monitor.forwardedMessagesMutex.Unlock()
}
