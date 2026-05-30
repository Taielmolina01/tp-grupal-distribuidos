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

type Msg[T any] struct {
	ClientID int
	QueryID  uint8
	EOF      bool
	Total    uint32
	Records  []T
}

type Info struct {
	ClientID int
	QueryID  uint8
	EOF      bool
	Total    uint32
}

func WriteRaw(clientID int, queryID uint8, count uint16, payload []byte) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeBatch}.WriteTo(w)
	w.Uint16(count)
	return append(w.Bytes(), payload...)
}

func Write[T any](clientID int, queryID uint8, records []T, codec wire.Codec[T]) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeBatch}.WriteTo(w)
	w.Uint16(uint16(len(records)))
	for i := range records {
		codec.Marshal(w, &records[i])
	}
	return w.Bytes()
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	w := wire.NewWriter()
	envelope.Header{ClientID: clientID, QueryID: queryID, Type: typeEOF}.WriteTo(w)
	w.Uint32(total)
	return w.Bytes()
}

func ReadHeader(body []byte) (*wire.Reader, Info, error) {
	r := wire.NewReader(body)
	h := envelope.ReadFrom(r)

	switch h.Type {
	case typeBatch:
		return r, Info{ClientID: h.ClientID, QueryID: h.QueryID}, r.Err()
	case typeEOF:
		total := r.Uint32()
		if err := r.Err(); err != nil {
			return r, Info{}, err
		}
		return r, Info{ClientID: h.ClientID, QueryID: h.QueryID, EOF: true, Total: total}, nil
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
		return Msg[T]{ClientID: info.ClientID, QueryID: info.QueryID, EOF: true, Total: info.Total}, nil
	}
	records, err := ReadRecords(r, codec)
	if err != nil {
		return Msg[T]{}, err
	}
	return Msg[T]{ClientID: info.ClientID, QueryID: info.QueryID, Records: records}, nil
}
