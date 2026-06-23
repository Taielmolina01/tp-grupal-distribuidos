package q3filter

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.TransferForQ3Filter]

var Codec = wire.Codec[transfer.TransferForQ3Filter]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   3*wire.Uint16Size + wire.Uint64Size,
}

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, records []transfer.TransferForQ3Filter) []byte {
	return batch.Write(clientID, queryID, senderID, seq, records, Codec)
}

func WriteEOF(clientID int, queryID uint8, senderID uint8, seq uint64, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, senderID, seq, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, Codec)
}

func marshalRecord(w *wire.Writer, t *transfer.TransferForQ3Filter) {
	w.String(t.PaymentFormat)
	w.Float64(t.AmountPaid)
	w.String(t.FromBank)
	w.String(t.FromBankAccount)
}

func unmarshalRecord(r *wire.Reader) transfer.TransferForQ3Filter {
	return transfer.TransferForQ3Filter{
		PaymentFormat:   r.String(),
		AmountPaid:      r.Float64(),
		FromBank:        r.String(),
		FromBankAccount: r.String(),
	}
}
