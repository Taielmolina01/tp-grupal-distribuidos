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
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

const nextClientIDKey = "next"
const pendingAbortsKey = "aborts"

const seqStoreCapacity uint64 = 10_000_000

type sessionState struct {
	phase        tcpproto.Phase
	trackers     map[uint8]*sendertracker.SenderTracker
	reported     map[uint8]struct{}
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
		trackers:     map[uint8]*sendertracker.SenderTracker{},
		reported:     map[uint8]struct{}{},
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

func (s *sessionStore) trackerFor(state *sessionState, queryID uint8) *sendertracker.SenderTracker {
	t, ok := state.trackers[queryID]
	if !ok {
		t = sendertracker.New(seqStoreCapacity)
		state.trackers[queryID] = t
	}
	return t
}

func (s *sessionStore) isDuplicateResult(clientID int, queryID uint8, senderID uint8, seq uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return false
	}
	t, ok := state.trackers[queryID]
	if !ok {
		return false
	}
	return t.IsDuplicate(int(senderID), seq)
}

func (s *sessionStore) claimResult(clientID int, queryID uint8, senderID uint8, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil
	}
	t := s.trackerFor(state, queryID)
	if t.IsDuplicate(int(senderID), seq) {
		return nil
	}
	t.Claim(int(senderID), seq)
	t.RegisterBatch(int(senderID))
	return nil
}

func (s *sessionStore) registerEOFResult(clientID int, queryID uint8, senderID uint8, total uint64, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil
	}
	t := s.trackerFor(state, queryID)
	t.RegisterEOF(int(senderID), total, seq)
	return nil
}

func (s *sessionStore) queryReported(clientID int, queryID uint8) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return false
	}
	_, reported := state.reported[queryID]
	return reported
}

func (s *sessionStore) reportedQueries(clientID int) []uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return nil
	}
	ids := make([]uint8, 0, len(state.reported))
	for queryID := range state.reported {
		ids = append(ids, queryID)
	}
	return ids
}

func (s *sessionStore) queryComplete(clientID int, queryID uint8, expectedSenders int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return false
	}
	t, ok := state.trackers[queryID]
	if !ok {
		return false
	}
	return t.IsComplete(expectedSenders)
}

func (s *sessionStore) markReported(clientID int, queryID uint8, totalQueries int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[clientID]
	if !ok {
		return false, nil
	}
	state.reported[queryID] = struct{}{}
	allReported := len(state.reported) >= totalQueries
	return allReported, s.persist()
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

func (s *sessionStore) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist()
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

		w.Uint32(uint32(len(state.reported)))
		for queryID := range state.reported {
			w.Uint8(queryID)
		}

		w.Uint32(uint32(len(state.trackers)))
		for queryID, tracker := range state.trackers {
			w.Uint8(queryID)
			tracker.Marshal(&w)
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

		reportedLen := r.Uint32()
		if r.Err() != nil {
			return r.Err()
		}
		reported := make(map[uint8]struct{}, reportedLen)
		for range reportedLen {
			reported[r.Uint8()] = struct{}{}
		}
		if r.Err() != nil {
			return r.Err()
		}

		trackersLen := r.Uint32()
		if r.Err() != nil {
			return r.Err()
		}
		trackers := make(map[uint8]*sendertracker.SenderTracker, trackersLen)
		for range trackersLen {
			queryID := r.Uint8()
			tracker, err := sendertracker.Unmarshal(r)
			if err != nil {
				return err
			}
			trackers[queryID] = tracker
		}

		confirmedSeq := map[tcpproto.Phase]uint64{}
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

		s.sessions[clientID] = &sessionState{phase: phase, trackers: trackers, reported: reported, confirmedSeq: confirmedSeq}
	}
	return nil
}
