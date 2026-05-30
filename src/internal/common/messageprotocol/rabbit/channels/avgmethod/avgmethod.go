package avgmethod

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.AvgByMethod]

var codec = wire.Codec[transfer.AvgByMethod]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   serializer.UINT64_SIZE + serializer.UINT16_SIZE,
}

func WriteBatch(clientID int, queryID uint8, records []transfer.AvgByMethod) []byte {
	return batch.Write(clientID, queryID, records, codec)
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, total)
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
