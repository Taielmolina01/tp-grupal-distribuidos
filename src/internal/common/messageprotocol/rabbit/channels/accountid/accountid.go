package accountid

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
)

type Msg = batch.Msg[account.AccountIdentifier]

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, ids []account.AccountIdentifier) []byte {
	return batch.Write(clientID, queryID, senderID, seq, ids, records.AccountIdentifierCodec)
}

func WriteEOF(clientID int, queryID uint8, senderID uint8, seq uint64, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, senderID, seq, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, records.AccountIdentifierCodec)
}
