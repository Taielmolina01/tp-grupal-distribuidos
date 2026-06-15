package seqstore

import (
	"encoding/binary"
	"fmt"
)

type SeqStore struct {
	claimed []uint64
}

func New(capacity uint64) *SeqStore {
	words := (capacity + 63) / 64
	return &SeqStore{
		claimed: make([]uint64, words),
	}
}

func (s *SeqStore) IsClaimed(seq uint64) bool {
	return s.isClaimed(seq)
}

func (s *SeqStore) Claim(seq uint64) bool {
	if s.isClaimed(seq) {
		return false
	}
	s.claimed[seq/64] |= 1 << (seq % 64)
	return true
}

func (s *SeqStore) isClaimed(seq uint64) bool {
	return (s.claimed[seq/64]>>(seq%64))&1 == 1
}

func (s *SeqStore) Marshal() []byte {
	buf := make([]byte, len(s.claimed)*8)
	for i, v := range s.claimed {
		binary.BigEndian.PutUint64(buf[i*8:], v)
	}
	return buf
}

func NewFromBytes(data []byte) (*SeqStore, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("seqstore: invalid data length %d", len(data))
	}
	words := len(data) / 8
	claimed := make([]uint64, words)
	for i := range words {
		claimed[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return &SeqStore{claimed: claimed}, nil
}
