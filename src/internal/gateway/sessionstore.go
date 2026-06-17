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
const pendingAbortsKey = "aborts"

type sessionState struct {
	phase        tcpproto.Phase
	eofCounts    map[uint8]int
	confirmedSeq map[tcpproto.Phase]uint64
}

type sessionStore struct {
	mu            sync.Mutex
	path          string
	nextClientID  int32
	sessions      map[int]*sessionState
	pendingAborts map[int]struct{}
}

func newSessionStore(path string) (*sessionStore, error) {
	s := &sessionStore{
		path:          path,
		sessions:      map[int]*sessionState{},
		pendingAborts: map[int]struct{}{},
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
	s.sessions[clientID] = &sessionState{
		phase:        tcpproto.PhaseAccounts,
		eofCounts:    map[uint8]int{},
		confirmedSeq: map[tcpproto.Phase]uint64{},
	}
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

func (s *sessionStore) advanceConfirmedSeq(clientID int, phase tcpproto.Phase, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil
	}
	if seq <= state.confirmedSeq[phase] {
		return nil
	}
	state.confirmedSeq[phase] = seq
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

func (s *sessionStore) moveToPendingAbort(clientID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, clientID)
	s.pendingAborts[clientID] = struct{}{}
	return s.persist()
}

func (s *sessionStore) clearPendingAbort(clientID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pendingAborts, clientID)
	return s.persist()
}

func (s *sessionStore) sessionIDs() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]int, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	return ids
}

func (s *sessionStore) pendingAbortIDs() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]int, 0, len(s.pendingAborts))
	for id := range s.pendingAborts {
		ids = append(ids, id)
	}
	return ids
}

func (s *sessionStore) persist() error {
	if s.path == "" {
		return nil
	}

	data := make(map[string][]byte, len(s.sessions)+2)
	data[nextClientIDKey] = wire.AppendUint32(nil, uint32(s.nextClientID))

	var aborts wire.Writer
	aborts.Uint32(uint32(len(s.pendingAborts)))
	for clientID := range s.pendingAborts {
		aborts.Uint32(uint32(clientID))
	}
	data[pendingAbortsKey] = aborts.Bytes()

	for clientID, state := range s.sessions {
		var w wire.Writer
		w.Uint8(uint8(state.phase))
		w.Uint32(uint32(len(state.eofCounts)))
		for queryID, count := range state.eofCounts {
			w.Uint8(queryID)
			w.Uint32(uint32(count))
		}
		w.Uint32(uint32(len(state.confirmedSeq)))
		for phase, seq := range state.confirmedSeq {
			w.Uint8(uint8(phase))
			w.Uint64(seq)
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

	if raw, ok := data[pendingAbortsKey]; ok {
		r := wire.NewReader(raw)
		n := r.Uint32()
		if r.Err() != nil {
			return r.Err()
		}
		for range n {
			s.pendingAborts[int(r.Uint32())] = struct{}{}
		}
		if r.Err() != nil {
			return r.Err()
		}
	}

	for key, raw := range data {
		if key == nextClientIDKey || key == pendingAbortsKey {
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

		confirmedSeq := map[tcpproto.Phase]uint64{}
		if r.Remaining() > 0 {
			seqLen := r.Uint32()
			if r.Err() != nil {
				return r.Err()
			}
			for range seqLen {
				p := tcpproto.Phase(r.Uint8())
				seq := r.Uint64()
				confirmedSeq[p] = seq
			}
			if r.Err() != nil {
				return r.Err()
			}
		}

		s.sessions[clientID] = &sessionState{phase: phase, eofCounts: eofCounts, confirmedSeq: confirmedSeq}
	}
	return nil
}
