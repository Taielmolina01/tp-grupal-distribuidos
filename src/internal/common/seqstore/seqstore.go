package seqstore

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
