package external

import (
	"io"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type BatchBuilder[T any] struct {
	records   []T
	byteSize  int
	maxBytes  int
	maxCount  int
	serialize func(*T) []byte
	write     func(io.Writer, []T) error
}

func (b *BatchBuilder[T]) TryAdd(record T) bool {
	size := len(b.serialize(&record))
	if !b.IsEmpty() && (len(b.records) >= b.maxCount || b.byteSize+size > b.maxBytes) {
		return false
	}
	b.records = append(b.records, record)
	b.byteSize += size
	return true
}

func (b *BatchBuilder[T]) IsEmpty() bool {
	return len(b.records) == 0
}

func (b *BatchBuilder[T]) Flush(w io.Writer) error {
	err := b.write(w, b.records)
	b.records = b.records[:0]
	b.byteSize = 0
	return err
}

func NewAccountBatchBuilder(maxCount, maxBytes int) *BatchBuilder[account.Account] {
	return &BatchBuilder[account.Account]{
		maxBytes:  maxBytes,
		maxCount:  maxCount,
		serialize: serializeAccountRecord,
		write:     WriteAccountBatch,
	}
}

func NewTransBatchBuilder(maxCount, maxBytes int) *BatchBuilder[transfer.Transfer] {
	return &BatchBuilder[transfer.Transfer]{
		maxBytes:  maxBytes,
		maxCount:  maxCount,
		serialize: serializeTransRecord,
		write:     WriteTransBatch,
	}
}
