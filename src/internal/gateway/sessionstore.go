package gateway

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"tp-grupal-distribuidos/internal/common/diskstore"
	"tp-grupal-distribuidos/internal/common/messageprotocol/tcpproto"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

const nextClientIDKey = "next"

type sessionState struct {
	phase     tcpproto.Phase
	eofCounts map[uint8]int
}

type sessionStore struct {
	mu           sync.Mutex
	path         string
	nextClientID int32
	sessions     map[int]*sessionState
}

func newSessionStore(path string) (*sessionStore, error) {
	s := &sessionStore{
		path:     path,
		sessions: map[int]*sessionState{},
	}
	if path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sessionStore) allocateClient() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextClientID++
	clientID := int(s.nextClientID)
	s.sessions[clientID] = &sessionState{phase: tcpproto.PhaseAccounts, eofCounts: map[uint8]int{}}
	return clientID, s.persist()
}

func (s *sessionStore) session(clientID int) (sessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return sessionState{}, false
	}
	return *state, true
}

func (s *sessionStore) setPhase(clientID int, phase tcpproto.Phase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil
	}
	state.phase = phase
	return s.persist()
}

func (s *sessionStore) incEOF(clientID int, queryID uint8) (map[uint8]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil, nil
	}
	state.eofCounts[queryID]++
	snapshot := make(map[uint8]int, len(state.eofCounts))
	for q, c := range state.eofCounts {
		snapshot[q] = c
	}
	return snapshot, s.persist()
}

func (s *sessionStore) removeClient(clientID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, clientID)
	return s.persist()
}

func (s *sessionStore) persist() error {
	if s.path == "" {
		return nil
	}

	data := make(map[string][]byte, len(s.sessions)+1)
	data[nextClientIDKey] = wire.AppendUint32(nil, uint32(s.nextClientID))

	for clientID, state := range s.sessions {
		var w wire.Writer
		w.Uint8(uint8(state.phase))
		w.Uint32(uint32(len(state.eofCounts)))
		for queryID, count := range state.eofCounts {
			w.Uint8(queryID)
			w.Uint32(uint32(count))
		}
		data[strconv.Itoa(clientID)] = w.Bytes()
	}

	return diskstore.WriteAtomic(s.path, data)
}

func (s *sessionStore) load() error {
	if s.path == "" {
		return nil
	}
	data, err := diskstore.Read(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	if raw, ok := data[nextClientIDKey]; ok {
		s.nextClientID = int32(wire.NewReader(raw).Uint32())
	}

	for key, raw := range data {
		if key == nextClientIDKey {
			continue
		}
		clientID, err := strconv.Atoi(key)
		if err != nil {
			continue
		}

		r := wire.NewReader(raw)
		phase := tcpproto.Phase(r.Uint8())
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
		s.sessions[clientID] = &sessionState{phase: phase, eofCounts: eofCounts}
	}
	return nil
}
