package reducer

import (
	"sync"
	"tp-grupal-distribuidos/internal/common/msgmonitor"
)

type ReducerMonitorImpl[T comparable] struct {
	lock            sync.Mutex
	actualValues    map[int]map[string]T
	handlerMessages msgmonitor.MessageMonitor
}

type ReducerMonitor[T comparable] interface {
	GetValuesCopyByClientIdAndDelete(clientId int) map[string]T
	AddValue(clientId int, key string, value T, reducerFunction func(T, T) T)
}

func NewReducerMonitor[T comparable](
	handlerMessages msgmonitor.MessageMonitor,
) ReducerMonitor[T] {
	return &ReducerMonitorImpl[T]{
		actualValues:    map[int]map[string]T{},
		handlerMessages: handlerMessages,
	}
}

func (rm *ReducerMonitorImpl[T]) GetValuesCopyByClientIdAndDelete(clientId int) map[string]T {
	rm.lock.Lock()
	defer rm.lock.Unlock()

	values := rm.actualValues[clientId]
	delete(rm.actualValues, clientId)
	return values
}

func (rm *ReducerMonitorImpl[T]) AddValue(
	clientId int,
	key string,
	value T,
	reducerFunction func(T, T) T,
) {
	rm.lock.Lock()
	defer rm.lock.Unlock()

	if rm.actualValues[clientId] == nil {
		rm.actualValues[clientId] = map[string]T{}
	}
	existing, ok := rm.actualValues[clientId][key]
	if !ok {
		rm.actualValues[clientId][key] = value
		rm.handlerMessages.AddForwardedMessagesAmountByClientId(clientId, 1)
	} else {
		rm.actualValues[clientId][key] = reducerFunction(existing, value)
	}
	rm.handlerMessages.AddProcessedMessagesAmountByClientId(clientId, 1)
}
