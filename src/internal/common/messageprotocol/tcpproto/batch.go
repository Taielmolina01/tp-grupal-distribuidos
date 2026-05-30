package tcpproto

import (
	"io"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/byteconv"
	"tp-grupal-distribuidos/internal/common/messageprotocol/safeio"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

const maxBatchBytes = 64 * 1024

type BatchBuilder[T any] struct {
	buf             []byte
	count           int
	maxCount        int
	msgType         uint8
	withPayloadSize bool
	marshal         func([]byte, *T) []byte
}

func (b *BatchBuilder[T]) TryAdd(record T) bool {
	start := len(b.buf)
	b.buf = b.marshal(b.buf, &record)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *BatchBuilder[T]) IsEmpty() bool { return b.count == 0 }

func (b *BatchBuilder[T]) Flush(w io.Writer) error {
	var hdr [7]byte
	h := byteconv.AppendUint8(hdr[:0], b.msgType)
	h = byteconv.AppendUint16(h, uint16(b.count))
	if b.withPayloadSize {
		h = byteconv.AppendUint32(h, uint32(len(b.buf)))
	}
	if err := safeio.WriteAll(w, h); err != nil {
		return err
	}
	if err := safeio.WriteAll(w, b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	b.count = 0
	return nil
}

func NewAccountBatchBuilder(maxCount int) *BatchBuilder[account.Account] {
	return &BatchBuilder[account.Account]{
		maxCount: maxCount,
		msgType:  uint8(AccountBatch),
		marshal:  marshalAccountRecord,
	}
}

func NewTransBatchBuilder(maxCount int) *BatchBuilder[transfer.Transfer] {
	return &BatchBuilder[transfer.Transfer]{
		maxCount:        maxCount,
		msgType:         uint8(TransBatch),
		withPayloadSize: true,
		marshal:         marshalTransRecord,
	}
}

type ResultBatchBuilder struct {
	buf      []byte
	count    int
	maxCount int
}

func NewResultBatchBuilder(maxCount int) *ResultBatchBuilder {
	return &ResultBatchBuilder{maxCount: maxCount}
}

func (b *ResultBatchBuilder) IsEmpty() bool { return b.count == 0 }

func (b *ResultBatchBuilder) TryAddQuery1(r queryresult.Query1Result) bool {
	start := len(b.buf)
	b.buf = serializeQuery1Result(append(b.buf, 1), &r)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *ResultBatchBuilder) TryAddQuery2(r queryresult.Query2Result) bool {
	start := len(b.buf)
	b.buf = serializeQuery2Result(append(b.buf, 2), &r)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *ResultBatchBuilder) TryAddQuery3(r queryresult.Query3Result) bool {
	start := len(b.buf)
	b.buf = serializeQuery3Result(append(b.buf, 3), &r)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *ResultBatchBuilder) TryAddQuery4(r queryresult.Query4Result) bool {
	start := len(b.buf)
	b.buf = serializeQuery4Result(append(b.buf, 4), &r)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *ResultBatchBuilder) TryAddQuery5(r queryresult.Query5Result) bool {
	start := len(b.buf)
	b.buf = serializeQuery5Result(append(b.buf, 5), &r)
	if b.count > 0 && (b.count >= b.maxCount || len(b.buf) > maxBatchBytes) {
		b.buf = b.buf[:start]
		return false
	}
	b.count++
	return true
}

func (b *ResultBatchBuilder) Flush(w io.Writer) error {
	var hdr [3]byte
	h := byteconv.AppendUint8(hdr[:0], uint8(ResultBatch))
	h = byteconv.AppendUint16(h, uint16(b.count))
	if err := safeio.WriteAll(w, h); err != nil {
		return err
	}
	if err := safeio.WriteAll(w, b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	b.count = 0
	return nil
}
