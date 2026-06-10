package msgmonitor

import (
	"os"
	"sync"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
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
	SaveToDisk(path string) error
	LoadFromDisk(path string) error
}

type clientState struct {
	processed   uint32
	forwarded   uint32
	seqSent     uint64
	seqReceived map[int]uint64
}

type messageMonitorImpl struct {
	clients map[int]clientState
	// Ver la idea del snapshot
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
	if seq == 0 {
		return false
	}
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

func (m *messageMonitorImpl) SaveToDisk(path string) error {
	if path == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var w wire.Writer
	for clientID, state := range m.clients {
		w.Int32(int32(clientID))
		w.Uint32(state.processed)
		w.Uint32(state.forwarded)
		w.Uint64(state.seqSent)
		w.Uint32(uint32(len(state.seqReceived)))
		for senderID, val := range state.seqReceived {
			w.Int32(int32(senderID))
			w.Uint64(val)
		}
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *messageMonitorImpl) LoadFromDisk(path string) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	r := wire.NewReader(data)
	for r.Remaining() > 0 {
		clientID := int(r.Int32())
		processed := r.Uint32()
		forwarded := r.Uint32()
		seqSent := r.Uint64()
		seqRecvLen := r.Uint32()
		if r.Err() != nil {
			return r.Err()
		}

		seqReceived := make(map[int]uint64, seqRecvLen)
		for range seqRecvLen {
			senderID := int(r.Int32())
			val := r.Uint64()
			seqReceived[senderID] = val
		}
		if r.Err() != nil {
			return r.Err()
		}

		m.clients[clientID] = clientState{
			processed:   processed,
			forwarded:   forwarded,
			seqSent:     seqSent,
			seqReceived: seqReceived,
		}
	}
	return nil
}
