package external

import (
	"io"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const maxBatchBytes = 8 * 1024

type BatchBuilder[T any] struct {
	records   []T
	byteSize  int
	maxCount  int
	serialize func(*T) []byte
	write     func(io.Writer, []T) error
}

func (b *BatchBuilder[T]) TryAdd(record T) bool {
	size := len(b.serialize(&record))
	if !b.IsEmpty() && (len(b.records) >= b.maxCount || b.byteSize+size > maxBatchBytes) {
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

func NewAccountBatchBuilder(maxCount int) *BatchBuilder[account.Account] {
	return &BatchBuilder[account.Account]{
		maxCount:  maxCount,
		serialize: serializeAccountRecord,
		write:     WriteAccountBatch,
	}
}

func NewTransBatchBuilder(maxCount int) *BatchBuilder[transfer.Transfer] {
	return &BatchBuilder[transfer.Transfer]{
		maxCount:  maxCount,
		serialize: serializeTransRecord,
		write:     WriteTransBatch,
	}
}

type ResultBatchBuilder struct {
	batch    queryresult.BatchResults
	byteSize int
	maxBytes int
	maxCount int
}

func NewResultBatchBuilder(maxCount, maxBytes int) *ResultBatchBuilder {
	return &ResultBatchBuilder{maxCount: maxCount, maxBytes: maxBytes}
}

func (b *ResultBatchBuilder) count() int {
	return len(b.batch.Query1) + len(b.batch.Query2) + len(b.batch.Query3) +
		len(b.batch.Query4) + len(b.batch.Query5)
}

func (b *ResultBatchBuilder) IsEmpty() bool {
	return b.count() == 0
}

func (b *ResultBatchBuilder) tryAdd(itemPayloadSize int) bool {
	size := 1 + itemPayloadSize
	if !b.IsEmpty() && (b.count() >= b.maxCount || b.byteSize+size > maxBatchBytes) {
		return false
	}
	b.byteSize += size
	return true
}

func (b *ResultBatchBuilder) TryAddQuery1(r queryresult.Query1Result) bool {
	if !b.tryAdd(len(serializeQuery1Result(&r))) {
		return false
	}
	b.batch.Query1 = append(b.batch.Query1, r)
	return true
}

func (b *ResultBatchBuilder) TryAddQuery2(r queryresult.Query2Result) bool {
	if !b.tryAdd(len(serializeQuery2Result(&r))) {
		return false
	}
	b.batch.Query2 = append(b.batch.Query2, r)
	return true
}

func (b *ResultBatchBuilder) TryAddQuery3(r queryresult.Query3Result) bool {
	if !b.tryAdd(len(serializeQuery3Result(&r))) {
		return false
	}
	b.batch.Query3 = append(b.batch.Query3, r)
	return true
}

func (b *ResultBatchBuilder) TryAddQuery4(r queryresult.Query4Result) bool {
	if !b.tryAdd(len(serializeQuery4Result(&r))) {
		return false
	}
	b.batch.Query4 = append(b.batch.Query4, r)
	return true
}

func (b *ResultBatchBuilder) TryAddQuery5(r queryresult.Query5Result) bool {
	if !b.tryAdd(len(serializeQuery5Result(&r))) {
		return false
	}
	b.batch.Query5 = append(b.batch.Query5, r)
	return true
}

func (b *ResultBatchBuilder) Flush(w io.Writer) error {
	if err := WriteResultBatch(w, &b.batch); err != nil {
		return err
	}
	b.batch = queryresult.BatchResults{}
	b.byteSize = 0
	return nil
}
