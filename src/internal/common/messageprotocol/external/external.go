package external

import (
	"io"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/account"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/messageprotocol/external/safeio"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/messageprotocol/external/serializer"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
)

type MsgType uint32

const (
	Ack MsgType = iota + 1
	AccountBatch
	TransBatch
	EndOfRecords
)

func writeMsgType(writer io.Writer, msgType MsgType) error {
	msg := serializer.SerializeUint32(uint32(msgType))
	return safeio.WriteAll(writer, msg)
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeSerialized, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	msgType := MsgType(serializer.DeserializeUint32(msgTypeSerialized))
	return msgType, nil
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}

func serializeAccountRecord(acc *account.Account) []byte {
	msg := serializer.SerializeString(acc.BankName)
	msg = append(msg, serializer.SerializeString(acc.BankId)...)
	msg = append(msg, serializer.SerializeString(acc.AccountNumber)...)
	msg = append(msg, serializer.SerializeString(acc.EntityId)...)
	msg = append(msg, serializer.SerializeString(acc.EntityName)...)
	return msg
}

func WriteAccountBatch(writer io.Writer, accounts []account.Account) error {
	msg := serializer.SerializeUint32(uint32(AccountBatch))
	msg = append(msg, serializer.SerializeUint32(uint32(len(accounts)))...)
	for _, acc := range accounts {
		msg = append(msg, serializeAccountRecord(&acc)...)
	}
	return safeio.WriteAll(writer, msg)
}

func serializeTransRecord(trans *transfer.Transfer) []byte {
	msg := serializer.SerializeString(trans.Timestamp.Format(time.RFC3339))
	msg = append(msg, serializer.SerializeString(trans.FromBank)...)
	msg = append(msg, serializer.SerializeString(trans.FromBankAccount)...)
	msg = append(msg, serializer.SerializeString(trans.ToBank)...)
	msg = append(msg, serializer.SerializeString(trans.ToBankAccount)...)
	msg = append(msg, serializer.SerializeFloat32(trans.AmountReceived)...)
	msg = append(msg, serializer.SerializeString(trans.ReceivingCurrency)...)
	msg = append(msg, serializer.SerializeFloat32(trans.AmountPaid)...)
	msg = append(msg, serializer.SerializeString(trans.PaymentCurrency)...)
	msg = append(msg, serializer.SerializeString(trans.PaymentFormat)...)
	msg = append(msg, serializer.SerializeBool(trans.IsLaundering)...)
	return msg
}

func WriteTransBatch(writer io.Writer, trans []transfer.Transfer) error {
	msg := serializer.SerializeUint32(uint32(TransBatch))
	msg = append(msg, serializer.SerializeUint32(uint32(len(trans)))...)
	for _, t := range trans {
		msg = append(msg, serializeTransRecord(&t)...)
	}
	return safeio.WriteAll(writer, msg)
}
