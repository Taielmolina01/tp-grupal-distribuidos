package qualifiedaccount

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type QualifiedAccount struct {
	Account account.AccountIdentifier
	IsLeft  bool
}

type Msg = batch.Msg[QualifiedAccount]

var codec = wire.Codec[QualifiedAccount]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   records.MinSizeAccountIdentifier + serializer.BOOL_SIZE,
}

func WriteBatch(clientID int, queryID uint8, recs []QualifiedAccount) []byte {
	return batch.Write(clientID, queryID, recs, codec)
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func marshalRecord(w *wire.Writer, q *QualifiedAccount) {
	records.MarshalAccountIdentifier(w, &q.Account)
	w.Bool(q.IsLeft)
}

func unmarshalRecord(r *wire.Reader) QualifiedAccount {
	return QualifiedAccount{
		Account: records.UnmarshalAccountIdentifier(r),
		IsLeft:  r.Bool(),
	}
}
