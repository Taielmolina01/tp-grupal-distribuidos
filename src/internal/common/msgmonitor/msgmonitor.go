package msgmonitor

import (
	"os"
	"sync"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type ClientStatus uint8

const (
	StatusReady   ClientStatus = iota
	StatusClaim
	StatusConfirm
)

type MessageMonitor interface {
	GetProcessedMessagesAmountByClientId(int) uint32
	SetProcessedMessagesAmountByClientId(int, uint32)
	AddProcessedMessagesAmountByClientId(int, uint32)
	GetForwardedMessagesAmountByClientId(int) uint32
	SetForwardedMessagesAmountByClientId(int, uint32)
	AddForwardedMessagesAmountByClientId(int, uint32)
	GetProcessedOldByClientId(int) uint32
	SetProcessedOldByClientId(int, uint32)
	GetForwardedOldByClientId(int) uint32
	SetForwardedOldByClientId(int, uint32)
	GetLastSeqNumberByClientId(int) uint64
	SetLastSeqNumberByClientId(int, uint64)
	GetOutputsByClientId(int) map[string][]byte
	SetOutputsByClientId(int, map[string][]byte)
	GetStatusByClientId(int) ClientStatus
	SetStatusByClientId(int, ClientStatus)
	RemoveClient(int)
	Close()
	SaveToDisk(path string) error
	LoadFromDisk(path string) error
	Len() int
}

type clientState struct {
	processed    uint32
	forwarded    uint32
	processedOld uint32
	forwardedOld uint32
	lastSeqNumber uint64
	outputs       map[string][]byte
	status        ClientStatus
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

func (m *messageMonitorImpl) Len() int {
	return len(m.clients)
}
func (m *messageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].processed
}

func (m *messageMonitorImpl) SetProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.processed = amount
	m.clients[clientID] = s
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

func (m *messageMonitorImpl) SetForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.forwarded = amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) AddForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.forwarded += amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetProcessedOldByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].processedOld
}

func (m *messageMonitorImpl) SetProcessedOldByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.processedOld = amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetForwardedOldByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].forwardedOld
}

func (m *messageMonitorImpl) SetForwardedOldByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.forwardedOld = amount
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetLastSeqNumberByClientId(clientID int) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].lastSeqNumber
}

func (m *messageMonitorImpl) SetLastSeqNumberByClientId(clientID int, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.lastSeqNumber = seq
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetOutputsByClientId(clientID int) map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].outputs
}

func (m *messageMonitorImpl) SetOutputsByClientId(clientID int, outputs map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.outputs = outputs
	m.clients[clientID] = s
}

func (m *messageMonitorImpl) GetStatusByClientId(clientID int) ClientStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].status
}

func (m *messageMonitorImpl) SetStatusByClientId(clientID int, status ClientStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.clients[clientID]
	s.status = status
	m.clients[clientID] = s
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
		w.Uint32(state.processedOld)
		w.Uint32(state.forwardedOld)
		w.Uint64(state.lastSeqNumber)
		w.Uint8(uint8(state.status))
		w.Uint32(uint32(len(state.outputs)))
		for k, v := range state.outputs {
			w.String(k)
			w.ByteSlice(v)
		}
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *messageMonitorImpl) LoadFromDisk(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		clear(m.clients)
		return nil
	}
	if err != nil {
		return err
	}

	r := wire.NewReader(data)
	for r.Remaining() > 0 {
		clientID := int(r.Int32())
		processed := r.Uint32()
		forwarded := r.Uint32()
		processedOld := r.Uint32()
		forwardedOld := r.Uint32()
		lastSeqNumber := r.Uint64()
		status := ClientStatus(r.Uint8())
		outputCount := r.Count(6) // min 6 bytes per entry (uint16 key prefix + uint32 value prefix)
		var outputs map[string][]byte
		if outputCount > 0 {
			outputs = make(map[string][]byte, outputCount)
			for range outputCount {
				k := r.String()
				v := r.ByteSlice()
				outputs[k] = v
			}
		}

		m.clients[clientID] = clientState{
			processed:     processed,
			forwarded:     forwarded,
			processedOld:  processedOld,
			forwardedOld:  forwardedOld,
			lastSeqNumber: lastSeqNumber,
			status:        status,
			outputs:       outputs,
		}
	}
	if err := r.Err(); err != nil {
		return err
	}
	return nil
}
