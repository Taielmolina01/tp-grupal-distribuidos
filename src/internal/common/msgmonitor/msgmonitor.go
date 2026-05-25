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

type proccesedAndFiltered struct {
	processedMessages uint32
	forwardedMessages uint32
}

type messageMonitorImpl struct {
	messagesByClient       map[int]proccesedAndFiltered
	processedMessagesMutex sync.Mutex
	forwardedMessagesMutex sync.Mutex
}

func NewMessageMonitor() MessageMonitor {
	return &messageMonitorImpl{
		messagesByClient:       map[int]proccesedAndFiltered{},
		processedMessagesMutex: sync.Mutex{},
		forwardedMessagesMutex: sync.Mutex{},
	}
}

func (monitor *messageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	amount := monitor.messagesByClient[clientID].processedMessages
	return amount
}

func (monitor *messageMonitorImpl) AddProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	actual := monitor.messagesByClient[clientID]
	actual.processedMessages += amount
	monitor.messagesByClient[clientID] = actual
}

func (monitor *messageMonitorImpl) GetForwardedMessagesAmountByClientId(clientID int) uint32 {
	monitor.forwardedMessagesMutex.Lock()
	defer monitor.forwardedMessagesMutex.Unlock()

	amount := monitor.messagesByClient[clientID].forwardedMessages
	return amount
}

func (monitor *messageMonitorImpl) AddForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.forwardedMessagesMutex.Lock()
	defer monitor.forwardedMessagesMutex.Unlock()

	actual := monitor.messagesByClient[clientID]
	actual.forwardedMessages += amount
	monitor.messagesByClient[clientID] = actual
}

func (monitor *messageMonitorImpl) RemoveClient(clientID int) {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()
	delete(monitor.messagesByClient, clientID)
}

func (monitor *messageMonitorImpl) Close() {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	clear(monitor.messagesByClient)
}
