package external

import (
	"io"
	"log/slog"
	"time"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/external/safeio"
	"tp-grupal-distribuidos/internal/common/messageprotocol/external/serializer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
)

type MsgType uint8

const (
	Ack MsgType = iota + 1
	AccountBatch
	TransBatch
	EndOfRecords
	ResultBatch
	QueryEOF
)

func writeMsgType(writer io.Writer, msgType MsgType) error {
	return safeio.WriteAll(writer, serializer.SerializeUint8(uint8(msgType)))
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	b, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return 0, err
	}
	return MsgType(serializer.DeserializeUint8(b)), nil
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}

func WriteQueryEOF(writer io.Writer, queryId uint8) error {
	msg := serializer.SerializeUint8(uint8(QueryEOF))
	msg = append(msg, serializer.SerializeUint8(queryId)...)
	return safeio.WriteAll(writer, msg)
}

func ReadQueryEOF(reader io.Reader) (uint8, error) {
	b, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint8(b), nil
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
	msg := serializer.SerializeUint8(uint8(AccountBatch))
	msg = append(msg, serializer.SerializeUint16(uint16(len(accounts)))...)
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
	msg := serializer.SerializeUint8(uint8(TransBatch))
	msg = append(msg, serializer.SerializeUint16(uint16(len(trans)))...)
	for _, t := range trans {
		msg = append(msg, serializeTransRecord(&t)...)
	}
	return safeio.WriteAll(writer, msg)
}

func ReadResultBatch(reader io.Reader) (*queryresult.BatchResults, error) {
	count, err := readCount(reader)
	if err != nil {
		return nil, err
	}

	results := &queryresult.BatchResults{}

	for range count {
		queryIdBytes, err := safeio.ReadAll(reader, serializer.UINT8_SIZE)
		if err != nil {
			return nil, err
		}
		queryId := serializer.DeserializeUint8(queryIdBytes)

		switch queryId {
		case 1:
			item, err := deserializeQuery1Result(reader)
			if err != nil {
				return nil, err
			}
			results.Query1 = append(results.Query1, item)
		case 2:
			item, err := deserializeQuery2Result(reader)
			if err != nil {
				return nil, err
			}
			results.Query2 = append(results.Query2, item)
		case 3:
			item, err := deserializeQuery3Result(reader)
			if err != nil {
				return nil, err
			}
			results.Query3 = append(results.Query3, item)
		case 4:
			item, err := deserializeQuery4Result(reader)
			if err != nil {
				return nil, err
			}
			results.Query4 = append(results.Query4, item)
		case 5:
			item, err := deserializeQuery5Result(reader)
			if err != nil {
				return nil, err
			}
			results.Query5 = append(results.Query5, item)
		default:
			slog.Warn("Received unexpected query ID in result batch", "queryId", queryId)
		}
	}

	return results, nil
}

func readString(r io.Reader) (string, error) {
	lenBytes, err := safeio.ReadAll(r, serializer.UINT16_SIZE)
	if err != nil {
		return "", err
	}
	strLen := uint32(serializer.DeserializeUint16(lenBytes))
	strBytes, err := safeio.ReadAll(r, strLen)
	if err != nil {
		return "", err
	}
	return serializer.DeserializeString(strBytes), nil
}

func readCount(r io.Reader) (uint32, error) {
	b, err := safeio.ReadAll(r, serializer.UINT16_SIZE)
	if err != nil {
		return 0, err
	}
	return uint32(serializer.DeserializeUint16(b)), nil
}

func readFloat32(r io.Reader) (float32, error) {
	b, err := safeio.ReadAll(r, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeFloat32(b), nil
}

func deserializeQuery1Result(r io.Reader) (queryresult.Query1Result, error) {
	fromBank, err := readString(r)
	if err != nil {
		return queryresult.Query1Result{}, err
	}
	fromAccount, err := readString(r)
	if err != nil {
		return queryresult.Query1Result{}, err
	}
	toBank, err := readString(r)
	if err != nil {
		return queryresult.Query1Result{}, err
	}
	toAccount, err := readString(r)
	if err != nil {
		return queryresult.Query1Result{}, err
	}
	amount, err := readFloat32(r)
	if err != nil {
		return queryresult.Query1Result{}, err
	}
	return queryresult.Query1Result{
		FromBank:    fromBank,
		FromAccount: fromAccount,
		ToBank:      toBank,
		ToAccount:   toAccount,
		Amount:      amount,
	}, nil
}

func deserializeQuery2Result(r io.Reader) (queryresult.Query2Result, error) {
	bankName, err := readString(r)
	if err != nil {
		return queryresult.Query2Result{}, err
	}
	fromBank, err := readString(r)
	if err != nil {
		return queryresult.Query2Result{}, err
	}
	fromAccount, err := readString(r)
	if err != nil {
		return queryresult.Query2Result{}, err
	}
	amount, err := readFloat32(r)
	if err != nil {
		return queryresult.Query2Result{}, err
	}
	return queryresult.Query2Result{
		BankName:    bankName,
		FromBank:    fromBank,
		FromAccount: fromAccount,
		Amount:      amount,
	}, nil
}

func deserializeQuery3Result(r io.Reader) (queryresult.Query3Result, error) {
	fromBank, err := readString(r)
	if err != nil {
		return queryresult.Query3Result{}, err
	}
	fromAccount, err := readString(r)
	if err != nil {
		return queryresult.Query3Result{}, err
	}
	amount, err := readFloat32(r)
	if err != nil {
		return queryresult.Query3Result{}, err
	}
	return queryresult.Query3Result{
		FromBank:    fromBank,
		FromAccount: fromAccount,
		Amount:      amount,
	}, nil
}

func deserializeQuery4Result(r io.Reader) (queryresult.Query4Result, error) {
	bankId, err := readString(r)
	if err != nil {
		return queryresult.Query4Result{}, err
	}
	accountId, err := readString(r)
	if err != nil {
		return queryresult.Query4Result{}, err
	}
	return queryresult.Query4Result{BankId: bankId, AccountId: accountId}, nil
}

func deserializeQuery5Result(r io.Reader) (queryresult.Query5Result, error) {
	qty, err := readCount(r)
	if err != nil {
		return queryresult.Query5Result{}, err
	}
	return queryresult.Query5Result{Qty: qty}, nil
}
