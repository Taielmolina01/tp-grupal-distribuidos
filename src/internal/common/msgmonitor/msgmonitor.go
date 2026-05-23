package msgmonitor

import (
	"sync"
)

type MessageMonitor interface {
	GetProcessedMessagesAmountByClientId(int) uint32
	AddProcessedMessagesAmountByClientId(int, uint32)
	GetFilteredMessagesAmountByClientId(int) uint32
	AddFilteredMessagesAmountByClientId(int, uint32)
	RemoveClient(int)
	Close()
}

type proccesedAndFiltered struct {
	processedMessages uint32
	filteredMessages  uint32
}

type messageMonitorImpl struct {
	messagesByClient       map[int]proccesedAndFiltered
	processedMessagesMutex sync.Mutex
	filteredMessagesMutex  sync.Mutex
}

func NewMessageMonitor() MessageMonitor {
	return &messageMonitorImpl{
		messagesByClient:       map[int]proccesedAndFiltered{},
		processedMessagesMutex: sync.Mutex{},
		filteredMessagesMutex:  sync.Mutex{},
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

func (monitor *messageMonitorImpl) GetFilteredMessagesAmountByClientId(clientID int) uint32 {
	monitor.filteredMessagesMutex.Lock()
	defer monitor.filteredMessagesMutex.Unlock()
	amount := monitor.messagesByClient[clientID].filteredMessages
	return amount
}

func (monitor *messageMonitorImpl) AddFilteredMessagesAmountByClientId(clientID int, amount uint32) {
	monitor.filteredMessagesMutex.Lock()
	defer monitor.filteredMessagesMutex.Unlock()
	actual := monitor.messagesByClient[clientID]
	actual.filteredMessages += amount
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
