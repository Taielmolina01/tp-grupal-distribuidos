package accountid

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
)

type Msg = batch.Msg[account.AccountIdentifier]

func WriteBatch(clientID int, queryID uint8, ids []account.AccountIdentifier) []byte {
	return batch.Write(clientID, queryID, ids, records.AccountIdentifierCodec)
}

func WriteEOF(clientID int, queryID uint8, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, records.AccountIdentifierCodec)
}
