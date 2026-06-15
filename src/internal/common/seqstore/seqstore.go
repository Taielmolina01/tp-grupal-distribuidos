package seqstore

import (
	"encoding/binary"
	"fmt"
)

type SeqStore struct {
	store []uint64
}

func New(capacity uint64) *SeqStore {
	words := (capacity + 63) / 64
	return &SeqStore{
		store: make([]uint64, words),
	}
}

func (s *SeqStore) IsSet(seq uint64) bool {
	return s.isSet(seq)
}

func (s *SeqStore) Claim(seq uint64) bool {
	if s.isSet(seq) {
		return false
	}
	s.store[seq/64] |= 1 << (seq % 64)
	return true
}

func (s *SeqStore) isSet(seq uint64) bool {
	return (s.store[seq/64]>>(seq%64))&1 == 1
}

func (s *SeqStore) Marshal() []byte {
	buf := make([]byte, len(s.store)*8)
	for i, v := range s.store {
		binary.BigEndian.PutUint64(buf[i*8:], v)
	}
	return buf
}

func NewFromBytes(data []byte) (*SeqStore, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("seqstore: invalid data length %d", len(data))
	}
	words := len(data) / 8
	store := make([]uint64, words)
	for i := range words {
		store[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return &SeqStore{store: store}, nil
}
