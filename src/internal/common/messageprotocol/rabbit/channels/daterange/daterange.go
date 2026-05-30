// Package daterange is the protocol of the daterangesplitter -> sum channel.
// It carries batches of transfer.TransferForQ3Avg records, plus an EOF that
// closes the stream for a client.
//
// The batch/EOF framing lives in the batch package; this file only declares the
// record type and how to (de)serialize a single TransferForQ3Avg, which is the
// only part specific to this channel.
package daterange

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.TransferForQ3Avg]

var codec = wire.Codec[transfer.TransferForQ3Avg]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   serializer.UINT16_SIZE + serializer.UINT64_SIZE,
}

func WriteBatch(clientID int, queryID uint8, records []transfer.TransferForQ3Avg) []byte {
	return batch.Write(clientID, queryID, records, codec)
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func marshalRecord(w *wire.Writer, t *transfer.TransferForQ3Avg) {
	w.String(t.PaymentFormat)
	w.Float64(t.AmountPaid)
}

func unmarshalRecord(r *wire.Reader) transfer.TransferForQ3Avg {
	return transfer.TransferForQ3Avg{
		PaymentFormat: r.String(),
		AmountPaid:    r.Float64(),
	}
}
