package msgmonitor

import (
	"os"
	"sync"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type ShardedMessageMonitor interface {
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

	HandleEOF(clientID int, total uint32, senderID int)
	GetEOFInfo(clientID int) []EofInfo
}

type EofInfo struct {
	Processed uint32
}

type shardedClientState struct {
	processed   uint32
	forwarded   uint32
	seqSent     uint64
	seqReceived map[int]uint64
	eofs        map[int]EofInfo
}

type shardedMessageMonitorImpl struct {
	clients map[int]shardedClientState
	mu      sync.Mutex
}

func NewShardedMessageMonitor() ShardedMessageMonitor {
	return &shardedMessageMonitorImpl{
		clients: map[int]shardedClientState{},
	}
}

func (m *shardedMessageMonitorImpl) initClient(clientID int) {
	if _, ok := m.clients[clientID]; !ok {
		m.clients[clientID] = shardedClientState{
			processed:   0,
			forwarded:   0,
			seqSent:     0,
			seqReceived: map[int]uint64{},
			eofs:        map[int]EofInfo{},
		}
	}
}

func (m *shardedMessageMonitorImpl) GetProcessedMessagesAmountByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].processed
}

func (m *shardedMessageMonitorImpl) AddProcessedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initClient(clientID)
	s := m.clients[clientID]
	s.processed += amount
	m.clients[clientID] = s
}

func (m *shardedMessageMonitorImpl) GetForwardedMessagesAmountByClientId(clientID int) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[clientID].forwarded
}

func (m *shardedMessageMonitorImpl) AddForwardedMessagesAmountByClientId(clientID int, amount uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initClient(clientID)
	s := m.clients[clientID]
	s.forwarded += amount
	m.clients[clientID] = s
}

func (m *shardedMessageMonitorImpl) NextSeqByClientId(clientID int) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initClient(clientID)
	s := m.clients[clientID]
	s.seqSent++
	m.clients[clientID] = s
	return s.seqSent
}

func (m *shardedMessageMonitorImpl) IsDuplicate(clientID, senderID int, seq uint64) bool {
	if seq == 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initClient(clientID)
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

func (m *shardedMessageMonitorImpl) RemoveClient(clientID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, clientID)
}

func (m *shardedMessageMonitorImpl) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.clients)
}

func (m *shardedMessageMonitorImpl) SaveToDisk(path string) error {
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
		w.Uint8(uint8(len(state.eofs)))
		for senderID, eof := range state.eofs {
			w.Int32(int32(senderID))
			w.Uint32(eof.Processed)
		}
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, w.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *shardedMessageMonitorImpl) LoadFromDisk(path string) error {
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
		eofsLen := r.Uint8()
		eofs := make(map[int]EofInfo, eofsLen)
		for range eofsLen {
			senderID := int(r.Int32())
			processed := r.Uint32()
			eofs[senderID] = EofInfo{
				Processed: processed,
			}
		}
		if r.Err() != nil {
			return r.Err()
		}

		m.clients[clientID] = shardedClientState{
			processed:   processed,
			forwarded:   forwarded,
			seqSent:     seqSent,
			seqReceived: seqReceived,
			eofs:        eofs,
		}
	}
	return nil
}

func (m *shardedMessageMonitorImpl) HandleEOF(clientID int, total uint32, senderID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.clients[clientID]
	s.eofs[senderID] = EofInfo{
		Processed: uint32(total),
	}
	m.clients[clientID] = s
}

func (m *shardedMessageMonitorImpl) GetEOFInfo(clientID int) []EofInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	var eofs []EofInfo
	for _, eof := range m.clients[clientID].eofs {
		eofs = append(eofs, eof)
	}
	return eofs
}
