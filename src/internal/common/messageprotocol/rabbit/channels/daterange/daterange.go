package daterange

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.TransferForQ3Avg]

var codec = wire.Codec[transfer.TransferForQ3Avg]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   wire.Uint16Size + wire.Uint64Size,
}

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, records []transfer.TransferForQ3Avg) []byte {
	return batch.Write(clientID, queryID, senderID, seq, records, codec)
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
