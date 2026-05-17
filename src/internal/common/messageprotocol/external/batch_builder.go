package external

import (
	"io"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/account"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
)

type AccountBatchBuilder struct {
	records  []account.Account
	byteSize int
	maxBytes int
	maxCount int
}

func NewAccountBatchBuilder(maxCount, maxBytes int) *AccountBatchBuilder {
	return &AccountBatchBuilder{maxBytes: maxBytes, maxCount: maxCount}
}

func (b *AccountBatchBuilder) TryAdd(acc account.Account) bool {
	size := len(serializeAccountRecord(&acc))
	if !b.IsEmpty() && (len(b.records) >= b.maxCount || b.byteSize+size > b.maxBytes) {
		return false
	}
	b.records = append(b.records, acc)
	b.byteSize += size
	return true
}

func (b *AccountBatchBuilder) IsEmpty() bool {
	return len(b.records) == 0
}

func (b *AccountBatchBuilder) Flush(w io.Writer) error {
	err := WriteAccountBatch(w, b.records)
	b.records = b.records[:0]
	b.byteSize = 0
	return err
}

type TransBatchBuilder struct {
	records  []transfer.Transfer
	byteSize int
	maxBytes int
	maxCount int
}

func NewTransBatchBuilder(maxCount, maxBytes int) *TransBatchBuilder {
	return &TransBatchBuilder{maxBytes: maxBytes, maxCount: maxCount}
}

func (b *TransBatchBuilder) TryAdd(t transfer.Transfer) bool {
	size := len(serializeTransRecord(&t))
	if !b.IsEmpty() && (len(b.records) >= b.maxCount || b.byteSize+size > b.maxBytes) {
		return false
	}
	b.records = append(b.records, t)
	b.byteSize += size
	return true
}

func (b *TransBatchBuilder) IsEmpty() bool {
	return len(b.records) == 0
}

func (b *TransBatchBuilder) Flush(w io.Writer) error {
	err := WriteTransBatch(w, b.records)
	b.records = b.records[:0]
	b.byteSize = 0
	return err
}
