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
	prefix := len(s.claimed)
	for prefix > 0 && s.claimed[prefix-1] == 0 {
		prefix--
	}
	buf := make([]byte, 16+prefix*8)
	binary.BigEndian.PutUint64(buf[0:], uint64(len(s.claimed)))
	binary.BigEndian.PutUint64(buf[8:], uint64(prefix))
	for i := 0; i < prefix; i++ {
		binary.BigEndian.PutUint64(buf[16+i*8:], s.claimed[i])
	}
	return buf
}

func NewFromBytes(data []byte) (*SeqStore, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("seqstore: invalid data length %d", len(data))
	}
	capWords := binary.BigEndian.Uint64(data[0:])
	prefix := binary.BigEndian.Uint64(data[8:])
	if prefix > capWords || uint64(len(data)) != 16+prefix*8 {
		return nil, fmt.Errorf("seqstore: invalid data length %d", len(data))
	}
	claimed := make([]uint64, capWords)
	for i := uint64(0); i < prefix; i++ {
		claimed[i] = binary.BigEndian.Uint64(data[16+i*8:])
	}
	return &SeqStore{claimed: claimed}, nil
}
