package sum

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type MonitorSum interface {
	GetProccessedMessagesAmountByClientId(int) uint32
	CountNewFruitsFromClient(fruititem.FruitItemFromClient)
	GetFruitsByClientID(int) map[string]fruititem.FruitItem
	Close()
}

type MonitorSumImpl struct {
	fruitItemMapPerClient     map[int]map[string]fruititem.FruitItem
	processedMessagesByClient map[int]uint32
	fruitItemMutex            sync.Mutex
	processedMessagesMutex    sync.Mutex
}

func NewSumMonitor() MonitorSum {
	return &MonitorSumImpl{
		fruitItemMutex:            sync.Mutex{},
		processedMessagesMutex:    sync.Mutex{},
		fruitItemMapPerClient:     map[int]map[string]fruititem.FruitItem{},
		processedMessagesByClient: map[int]uint32{},
	}
}

func (monitor *MonitorSumImpl) GetProccessedMessagesAmountByClientId(clientID int) uint32 {
	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()
	amount, _ := monitor.processedMessagesByClient[clientID]
	return amount
}

func (monitor *MonitorSumImpl) CountNewFruitsFromClient(fruitsFromClient fruititem.FruitItemFromClient) {
	monitor.fruitItemMutex.Lock()

	if _, ok := monitor.fruitItemMapPerClient[fruitsFromClient.ClientId]; !ok {
		monitor.fruitItemMapPerClient[fruitsFromClient.ClientId] = map[string]fruititem.FruitItem{}
	}
	for _, fruitRecord := range fruitsFromClient.FruitItems {
		_, ok := monitor.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit]
		if ok {
			monitor.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit] = monitor.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			monitor.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}

	monitor.fruitItemMutex.Unlock()

	monitor.processedMessagesMutex.Lock()
	defer monitor.processedMessagesMutex.Unlock()

	_, ok := monitor.processedMessagesByClient[fruitsFromClient.ClientId]
	if ok {
		monitor.processedMessagesByClient[fruitsFromClient.ClientId] += 1
	} else {
		monitor.processedMessagesByClient[fruitsFromClient.ClientId] = 1
	}
}

func (monitor *MonitorSumImpl) GetFruitsByClientID(clientID int) map[string]fruititem.FruitItem {
	monitor.fruitItemMutex.Lock()
	defer monitor.fruitItemMutex.Unlock()

	return monitor.fruitItemMapPerClient[clientID]
}

func (monitor *MonitorSumImpl) Close() {
	clear(monitor.fruitItemMapPerClient)
	clear(monitor.processedMessagesByClient)
}
