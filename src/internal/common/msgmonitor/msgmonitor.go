package msgmonitor

import (
	"sync"
)

type MessageMonitor interface {
	GetProcessedMessagesAmountByClientId(int) uint32
	AddProcessedMessagesAmountByClientId(int, uint32)
	GetForwardedMessagesAmountByClientId(int) uint32
	AddForwardedMessagesAmountByClientId(int, uint32)
	NextSeqByClientId(clientID int) uint64
	IsDuplicate(clientID, senderID int, seq uint64) bool
	RemoveClient(int)
	Close()
}

type clientState struct {
	processed   uint32
	forwarded   uint32
	seqSent     uint64
	seqReceived map[int]uint64
}

type messageMonitorImpl struct {
	clients map[int]clientState
	mu      sync.Mutex
}

func NewMessageMonitor() MessageMonitor {
	return &messageMonitorImpl{
		clients: map[int]clientState{},
	}
}

func (m *messageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].processed
}

func (m *messageMonitorImpl) AddProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.processed += amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetForwardedMessagesAmountByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].forwarded
}

func (m *messageMonitorImpl) AddForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.forwarded += amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) NextSeqByClientId(clientID int) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.seqSent++
	m.clients[clientID] = s
	return s.seqSent
}

func (m *messageMonitorImpl) IsDuplicate(clientID, senderID int, seq uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	if s.seqReceived == nil {
		s.seqReceived = map[int]uint64{}
	}
	if seq <= s.seqReceived[senderID] {
		return true
	}
	s.seqReceived[senderID] = seq
	m.clients[clientID] = s
	return false
}

func (m *messageMonitorImpl) RemoveClient(clientID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, clientID)
}

func (m *messageMonitorImpl) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.clients)
}
