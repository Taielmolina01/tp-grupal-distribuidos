// Package splittransfer is the protocol of the filterandsplitter -> joinaccounts
// channel. It carries batches of transfer.SplittedTransfer (a TransferForQ4 plus
// the IsLeftPart flag), plus an EOF that closes the stream for a client.
//
// The batch/EOF framing lives in the batch package. The record embeds the shared
// TransferForQ4, whose codec is reused from the records package; this file owns
// only the channel-specific IsLeftPart field.
package splittransfer

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/serializer"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type Msg = batch.Msg[transfer.SplittedTransfer]

var codec = wire.Codec[transfer.SplittedTransfer]{
	Marshal:   marshalRecord,
	Unmarshal: unmarshalRecord,
	MinSize:   records.MinSizeTransferForQ4 + serializer.BOOL_SIZE,
}

func WriteBatch(clientID int, queryID uint8, senderID uint8, seq uint64, recs []transfer.SplittedTransfer) []byte {
	return batch.Write(clientID, queryID, senderID, seq, recs, codec)
}

func WriteEOF(clientID int, queryID uint8, senderID uint8, seq uint64, total uint32) []byte {
	return batch.WriteEOF(clientID, queryID, senderID, seq, total)
}

func Read(body []byte) (Msg, error) {
	return batch.Read(body, codec)
}

func marshalRecord(w *wire.Writer, t *transfer.SplittedTransfer) {
	records.MarshalTransferForQ4(w, &t.Transfer)
	w.Bool(t.IsLeftPart)
}

func unmarshalRecord(r *wire.Reader) transfer.SplittedTransfer {
	return transfer.SplittedTransfer{
		Transfer:   records.UnmarshalTransferForQ4(r),
		IsLeftPart: r.Bool(),
	}
}
