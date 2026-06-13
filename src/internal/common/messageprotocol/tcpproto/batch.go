package tcpproto

import (
	"io"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/safeio"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
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

func (b *BatchBuilder[T]) Reset() {
	b.buf = b.buf[:0]
	b.count = 0
}

func (b *BatchBuilder[T]) Flush(w io.Writer, seq uint64) error {
	var hdr [15]byte
	h := wire.AppendUint8(hdr[:0], b.msgType)
	h = wire.AppendUint64(h, seq)
	h = wire.AppendUint16(h, uint16(b.count))
	if b.withPayloadSize {
		h = wire.AppendUint32(h, uint32(len(b.buf)))
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

func WriteResultBatch(w io.Writer, queryID uint8, senderID uint8, seq uint64, count uint16, payload []byte) error {
	var hdr [13]byte
	h := wire.AppendUint8(hdr[:0], uint8(ResultBatch))
	h = wire.AppendUint8(h, queryID)
	h = wire.AppendUint8(h, senderID)
	h = wire.AppendUint64(h, seq)
	h = wire.AppendUint16(h, count)
	if err := safeio.WriteAll(w, h); err != nil {
		return err
	}
	return safeio.WriteAll(w, payload)
}
