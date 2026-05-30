package q3filter

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.TransferForQ3Filter]

var codec = wire.Codec[transfer.TransferForQ3Filter]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   3*serializer.UINT16_SIZE + serializer.UINT64_SIZE,
}

func WriteBatch(clientID int, queryID uint8, records []transfer.TransferForQ3Filter) []byte {
	return batch.Write(clientID, queryID, records, codec)
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
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
