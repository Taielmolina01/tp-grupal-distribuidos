package accountchain

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
)

type Msg = batch.Msg[account.AccountChain]

var codec = wire.Codec[account.AccountChain]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   3 * records.MinSizeAccountIdentifier,
}

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, chains []account.AccountChain) []byte {
	return batch.Write(clientID, queryID, senderID, seq, chains, codec)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func NewBatchBuilder(maxCount, maxBytes int) *batch.Builder[account.AccountChain] {
	return batch.NewBuilder(maxCount, maxBytes, codec)
}

func marshalRecord(w *wire.Writer, c *account.AccountChain) {
	records.MarshalAccountIdentifier(w, &c.Left)
	records.MarshalAccountIdentifier(w, &c.Middle)
	records.MarshalAccountIdentifier(w, &c.Right)
}

func unmarshalRecord(r *wire.Reader) account.AccountChain {
	return account.AccountChain{
		Left:   records.UnmarshalAccountIdentifier(r),
		Middle: records.UnmarshalAccountIdentifier(r),
		Right:  records.UnmarshalAccountIdentifier(r),
	}
}
