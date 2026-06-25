package avgmethod

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.AvgByMethod]

var codec = wire.Codec[transfer.AvgByMethod]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   wire.Uint64Size + wire.Uint16Size,
}

func NewBatchBuilder(maxCount, maxBytes int) *batch.Builder[transfer.AvgByMethod] {
	return batch.NewBuilder(maxCount, maxBytes, codec)
}

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, records []transfer.AvgByMethod) []byte {
	return batch.Write(clientID, queryID, senderID, seq, records, codec)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func marshalRecord(w *wire.Writer, a *transfer.AvgByMethod) {
	w.Float64(a.Avg)
	w.String(a.Method)
}

func unmarshalRecord(r *wire.Reader) transfer.AvgByMethod {
	return transfer.AvgByMethod{
		Avg:    r.Float64(),
		Method: r.String(),
	}
}
