package batch

import (
	"fmt"

	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/envelope"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

const (
	typeBatch uint8 = iota + 1
	typeEOF
)

type Builder[T any] struct {
	w        *wire.Writer
	count    int
	maxCount int
	maxBytes int
	codec    wire.Codec[T]
}

func NewBuilder[T any](maxCount, maxBytes int, codec wire.Codec[T]) *Builder[T] {
	return &Builder[T]{
		w:        wire.NewWriter(),
		maxCount: maxCount,
		maxBytes: maxBytes,
		codec:    codec,
	}
}

func (b *Builder[T]) TryAdd(record *T) bool {
	start := b.w.Len()
	b.codec.Marshal(b.w, record)
	if b.count > 0 && (b.count >= b.maxCount || b.w.Len() > b.maxBytes) {
		b.w.Truncate(start)
		return false
	}
	b.count++
	return true
}

func (b *Builder[T]) IsEmpty() bool { return b.count == 0 }

func (b *Builder[T]) Records() []T {
	r := wire.NewReader(b.w.Bytes())
	records := make([]T, 0, b.count)
	for range b.count {
		records = append(records, b.codec.Unmarshal(r))
	}
	return records
}

func (b *Builder[T]) Flush(clientID int, queryID uint8, senderID uint8, seq uint64) []byte {
	msg := WriteRaw(clientID, queryID, senderID, seq, uint16(b.count), b.w.Bytes())
	b.w.Reset()
	b.count = 0
	return msg
}

type Msg[T any] struct {
	ClientID int
	QueryID  uint8
	EOF      bool
	Total    uint32
	SenderID uint8
	Seq      uint64
	Records  []T
}

type Info struct {
	ClientID int
	QueryID  uint8
	EOF      bool
	Total    uint32
	SenderID uint8
	Seq      uint64
}

func WriteRaw(clientID int, queryID uint8, senderID uint8, seq uint64, count uint16, payload []byte) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeBatch, SenderID: senderID, Seq: seq}.WriteTo(w)
	w.Uint16(count)
	return append(w.Bytes(), payload...)
}

func Write[T any](clientID int, queryID uint8, senderID uint8, seq uint64, records []T, codec wire.Codec[T]) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeBatch, SenderID: senderID, Seq: seq}.WriteTo(w)
	w.Uint16(uint16(len(records)))
	for i := range records {
		codec.Marshal(w, &records[i])
	}
	return w.Bytes()
}

func WriteEOF(clientID int, queryID uint8, senderID uint8, seq uint64, total uint32) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeEOF, SenderID: senderID, Seq: seq}.WriteTo(w)
	w.Uint32(total)
	return w.Bytes()
}

func ReadHeader(body []byte) (*wire.Reader, Info, error) {
	r := wire.NewReader(body)
	h := envelope.ReadFrom(r)

	switch h.Type {
	case typeBatch:
		return r, Info{ClientID: h.ClientID, QueryID: h.QueryID, SenderID: h.SenderID, Seq: h.Seq}, r.Err()
	case typeEOF:
		total := r.Uint32()
		if err := r.Err(); err != nil {
			return r, Info{}, err
		}
		return r, Info{ClientID: h.ClientID, QueryID: h.QueryID, EOF: true, Total: total, SenderID: h.SenderID, Seq: h.Seq}, nil
	default:
		return r, Info{}, fmt.Errorf("batch: unknown message type %d", h.Type)
	}
}

func ReadRecords[T any](r *wire.Reader, codec wire.Codec[T]) ([]T, error) {
	n := r.Count(codec.MinSize)
	records := make([]T, 0, n)
	for range n {
		records = append(records, codec.Unmarshal(r))
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func Read[T any](body []byte, codec wire.Codec[T]) (Msg[T], error) {
	r, info, err := ReadHeader(body)
	if err != nil {
		return Msg[T]{}, err
	}
	if info.EOF {
		return Msg[T]{ClientID: info.ClientID, QueryID: info.QueryID, EOF: true, Total: info.Total, SenderID: info.SenderID, Seq: info.Seq}, nil
	}
	records, err := ReadRecords(r, codec)
	if err != nil {
		return Msg[T]{}, err
	}
	return Msg[T]{ClientID: info.ClientID, QueryID: info.QueryID, SenderID: info.SenderID, Seq: info.Seq, Records: records}, nil
}
