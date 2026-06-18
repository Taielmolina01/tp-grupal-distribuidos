package sendertracker

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/seqstore"
)

type SenderTracker struct {
	msgCount  map[int]uint32
	expected  map[int]uint32
	eofSet    map[int]struct{}
	seqStores map[int]*seqstore.SeqStore
	capacity  uint64
}

func New(seqCapacity uint64) *SenderTracker {
	return &SenderTracker{
		msgCount:  map[int]uint32{},
		expected:  map[int]uint32{},
		eofSet:    map[int]struct{}{},
		seqStores: map[int]*seqstore.SeqStore{},
		capacity:  seqCapacity,
	}
}

func (t *SenderTracker) IsDuplicate(senderID int, seq uint64) bool {
	store, ok := t.seqStores[senderID]
	if !ok {
		return false
	}
	return store.IsClaimed(seq)
}

func (t *SenderTracker) Claim(senderID int, seq uint64) {
	store, ok := t.seqStores[senderID]
	if !ok {
		store = seqstore.New(t.capacity)
		t.seqStores[senderID] = store
	}
	store.Claim(seq)
}

func (t *SenderTracker) RegisterBatch(senderID int, count uint32) {
	t.msgCount[senderID] += count
}

func (t *SenderTracker) RegisterEOF(senderID int, total uint32) {
	t.eofSet[senderID] = struct{}{}
	t.expected[senderID] = total
}

func (t *SenderTracker) IsComplete(expectedSenders int) bool {
	if len(t.eofSet) < expectedSenders {
		return false
	}
	for senderID, exp := range t.expected {
		if t.msgCount[senderID] < exp {
			return false
		}
	}
	return true
}

func (t *SenderTracker) TotalInput() uint32 {
	var total uint32
	for _, n := range t.expected {
		total += n
	}
	return total
}

func (t *SenderTracker) Marshal(w *wire.Writer) {
	w.Uint64(t.capacity)

	w.Uint32(uint32(len(t.msgCount)))
	for senderID, count := range t.msgCount {
		w.Int32(int32(senderID))
		w.Uint32(count)
	}
	w.Uint32(uint32(len(t.expected)))
	for senderID, exp := range t.expected {
		w.Int32(int32(senderID))
		w.Uint32(exp)
	}
	w.Uint32(uint32(len(t.eofSet)))
	for senderID := range t.eofSet {
		w.Int32(int32(senderID))
	}
	w.Uint32(uint32(len(t.seqStores)))
	for senderID, store := range t.seqStores {
		w.Int32(int32(senderID))
		data := store.Marshal()
		w.Uint32(uint32(len(data)))
		w.WriteRaw(data)
	}
}

func Unmarshal(r *wire.Reader) (*SenderTracker, error) {
	capacity := r.Uint64()

	n := r.Uint32()
	msgCount := make(map[int]uint32, n)
	for range n {
		msgCount[int(r.Int32())] = r.Uint32()
	}
	n = r.Uint32()
	expected := make(map[int]uint32, n)
	for range n {
		expected[int(r.Int32())] = r.Uint32()
	}
	n = r.Uint32()
	eofSet := make(map[int]struct{}, n)
	for range n {
		eofSet[int(r.Int32())] = struct{}{}
	}
	n = r.Uint32()
	seqStores := make(map[int]*seqstore.SeqStore, n)
	for range n {
		senderID := int(r.Int32())
		dataLen := r.Uint32()
		data := r.ReadRaw(dataLen)
		if r.Err() != nil {
			return nil, r.Err()
		}
		store, err := seqstore.NewFromBytes(data)
		if err != nil {
			return nil, err
		}
		seqStores[senderID] = store
	}

	if err := r.Err(); err != nil {
		return nil, err
	}

	return &SenderTracker{
		msgCount:  msgCount,
		expected:  expected,
		eofSet:    eofSet,
		seqStores: seqStores,
		capacity:  capacity,
	}, nil
}
