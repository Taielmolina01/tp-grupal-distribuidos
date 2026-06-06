package summethod

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.SumByMethod]

var codec = wire.Codec[transfer.SumByMethod]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   serializer.UINT64_SIZE + serializer.UINT32_SIZE + serializer.UINT16_SIZE,
}

func WriteBatch(clientID int, queryID uint8, senderID uint32, seq uint32, records []transfer.SumByMethod) []byte {
	return batch.Write(clientID, queryID, senderID, seq, records, codec)
}

func WriteEOF(clientID int, queryID uint8, senderID uint32, seq uint32, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, senderID, seq, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func marshalRecord(w *wire.Writer, s *transfer.SumByMethod) {
	w.Float64(s.Sum)
	w.Uint32(uint32(s.Amount))
	w.String(s.Method)
}

func unmarshalRecord(r *wire.Reader) transfer.SumByMethod {
	return transfer.SumByMethod{
		Sum:    r.Float64(),
		Amount: int(r.Uint32()),
		Method: r.String(),
	}
}
