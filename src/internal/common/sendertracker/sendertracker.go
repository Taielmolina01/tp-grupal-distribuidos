package sendertracker

import (
	"log/slog"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/seqstore"
)

type SenderTracker struct {
	msgCount  map[int]uint64
	expected  map[int]uint64
	eofSet    map[int]uint64
	seqStores map[int]*seqstore.SeqStore
	capacity  uint64
}

func New(seqCapacity uint64) *SenderTracker {
	return &SenderTracker{
		msgCount:  map[int]uint64{},
		expected:  map[int]uint64{},
		eofSet:    map[int]uint64{},
		seqStores: map[int]*seqstore.SeqStore{},
		capacity:  seqCapacity,
	}
}

func (t *SenderTracker) Clear() {

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

func (t *SenderTracker) RegisterBatch(senderID int) uint64 {
	t.msgCount[senderID]++
	return t.msgCount[senderID]
}

func (t *SenderTracker) RegisterEOF(senderID int, total uint64, seq uint64) {
	t.eofSet[senderID] = seq
	t.expected[senderID] = total
}

func (t *SenderTracker) GetEOFSeq() uint64 {
	for _, v := range t.eofSet {
		return v
	}
	return 0
}

func (t *SenderTracker) GetMsgCount() uint32 {
	var acum uint32
	for _, v := range t.msgCount {
		acum += uint32(int(v)) //ARREGLAR ESTO ACTUALIZANDO EL WIRE
	}
	return acum
}

func (t *SenderTracker) IsComplete(expectedSenders int) bool {
	slog.Info("Is Complete?", "len(t.eofSet)", len(t.eofSet), "expectedSenders", expectedSenders)
	if len(t.eofSet) < expectedSenders {
		slog.Info("Is Complete? FALSE")
		return false
	}
	for senderID, exp := range t.expected {
		if t.msgCount[senderID] < exp {
			slog.Info("Is Complete? FALSE 2")
			return false
		}
	}
	slog.Info("Is Complete? TRUE")
	return true
}

func (t *SenderTracker) TotalInput() uint64 {
	var total uint64
	for _, n := range t.expected {
		total += n
	}
	return total
}

func (t *SenderTracker) Marshal(w *wire.Writer) {
	w.Uint64(t.capacity)

	w.Uint64(uint64(len(t.msgCount)))
	for senderID, count := range t.msgCount {
		w.Int32(int32(senderID))
		w.Uint64(count)
	}
	w.Uint64(uint64(len(t.expected)))
	for senderID, exp := range t.expected {
		w.Int32(int32(senderID))
		w.Uint64(exp)
	}
	w.Uint64(uint64(len(t.eofSet)))
	for senderID := range t.eofSet {
		w.Int32(int32(senderID))
	}
	w.Uint64(uint64(len(t.seqStores)))
	for senderID, store := range t.seqStores {
		w.Int32(int32(senderID))
		data := store.Marshal()
		w.Uint64(uint64(len(data)))
		w.WriteRaw(data)
	}
}

func Unmarshal(r *wire.Reader) (*SenderTracker, error) {
	capacity := r.Uint64()

	n := r.Uint64()
	msgCount := make(map[int]uint64, n)
	for range n {
		msgCount[int(r.Int32())] = r.Uint64()
	}
	n = r.Uint64()
	expected := make(map[int]uint64, n)
	for range n {
		expected[int(r.Int32())] = r.Uint64()
	}
	n = r.Uint64()
	eofSet := make(map[int]uint64, n)
	for range n {
		// TODO: SERIALIZAR Y DESERIALIZAR USANDO ESTE VALOR
		eofSet[int(r.Int32())] = 0
	}
	n = r.Uint64()
	seqStores := make(map[int]*seqstore.SeqStore, n)
	for range n {
		senderID := int(r.Int32())
		dataLen := r.Uint64()
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
