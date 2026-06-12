package gateway

import (
	"os"
	"path/filepath"
	"sync"

	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type sessionState struct {
	phase     tcpproto.Phase
	lastSeq   uint64
	eofCounts map[uint8]int
}

type wal struct {
	mu           sync.Mutex
	path         string
	nextClientID int32
	sessions     map[int]*sessionState
	pending      int
	persistEvery int
}

func newWAL(path string, persistEvery int) (*wal, error) {
	if persistEvery <= 0 {
		persistEvery = 1
	}
	w := &wal{
		path:         path,
		sessions:     map[int]*sessionState{},
		persistEvery: persistEvery,
	}
	if path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
	}
	if err := w.load(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *wal) allocateClient() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextClientID++
	clientID := int(w.nextClientID)
	w.sessions[clientID] = &sessionState{phase: tcpproto.PhaseAccounts, eofCounts: map[uint8]int{}}
	return clientID, w.persist()
}

func (w *wal) session(clientID int) (sessionState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[clientID]
	if !ok {
		return sessionState{}, false
	}
	return *s, true
}

func (w *wal) advanceSeq(clientID int, seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[clientID]
	if !ok {
		return nil
	}
	s.lastSeq = seq
	w.pending++
	if w.pending < w.persistEvery {
		return nil
	}
	w.pending = 0
	return w.persist()
}

func (w *wal) completePhase(clientID int, eofSeq uint64, nextPhase tcpproto.Phase) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[clientID]
	if !ok {
		return nil
	}
	s.lastSeq = eofSeq
	s.phase = nextPhase
	w.pending = 0
	return w.persist()
}

func (w *wal) incEOF(clientID int, queryID uint8) (map[uint8]int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, ok := w.sessions[clientID]
	if !ok {
		return nil, nil
	}
	s.eofCounts[queryID]++
	snapshot := make(map[uint8]int, len(s.eofCounts))
	for q, c := range s.eofCounts {
		snapshot[q] = c
	}
	w.pending = 0
	return snapshot, w.persist()
}

func (w *wal) removeClient(clientID int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.sessions, clientID)
	w.pending = 0
	return w.persist()
}

func (w *wal) persist() error {
	if w.path == "" {
		return nil
	}
	var ww wire.Writer
	ww.Int32(w.nextClientID)
	ww.Uint32(uint32(len(w.sessions)))
	for clientID, s := range w.sessions {
		ww.Int32(int32(clientID))
		ww.Uint8(uint8(s.phase))
		ww.Uint64(s.lastSeq)
		ww.Uint32(uint32(len(s.eofCounts)))
		for queryID, count := range s.eofCounts {
			ww.Uint8(queryID)
			ww.Uint32(uint32(count))
		}
	}

	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, ww.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}

func (w *wal) load() error {
	if w.path == "" {
		return nil
	}
	data, err := os.ReadFile(w.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	r := wire.NewReader(data)
	w.nextClientID = r.Int32()
	sessionCount := r.Uint32()
	if r.Err() != nil {
		return r.Err()
	}

	for range sessionCount {
		clientID := int(r.Int32())
		phase := tcpproto.Phase(r.Uint8())
		lastSeq := r.Uint64()
		eofLen := r.Uint32()
		if r.Err() != nil {
			return r.Err()
		}
		eofCounts := make(map[uint8]int, eofLen)
		for range eofLen {
			queryID := r.Uint8()
			count := int(r.Uint32())
			eofCounts[queryID] = count
		}
		if r.Err() != nil {
			return r.Err()
		}
		w.sessions[clientID] = &sessionState{phase: phase, lastSeq: lastSeq, eofCounts: eofCounts}
	}
	return nil
}
