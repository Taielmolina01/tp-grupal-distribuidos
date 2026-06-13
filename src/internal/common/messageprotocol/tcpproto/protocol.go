package tcpproto

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/safeio"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

func writeMsgType(writer io.Writer, msgType MsgType) error {
	return safeio.WriteAll(writer, []byte{uint8(msgType)})
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	b, err := safeio.ReadAll(reader, 1)
	if err != nil {
		return 0, err
	}
	return MsgType(b[0]), nil
}

func WriteHello(writer io.Writer, sessionID uint32) error {
	var buf [5]byte
	b := wire.AppendUint8(buf[:0], uint8(Hello))
	b = wire.AppendUint32(b, sessionID)
	return safeio.WriteAll(writer, b)
}

func ReadHello(reader io.Reader) (uint32, error) {
	return readUint32(reader)
}

func WriteWelcome(writer io.Writer, sessionID uint32, phase Phase, nextSeq uint64) error {
	var buf [14]byte
	b := wire.AppendUint8(buf[:0], uint8(Welcome))
	b = wire.AppendUint32(b, sessionID)
	b = wire.AppendUint8(b, uint8(phase))
	b = wire.AppendUint64(b, nextSeq)
	return safeio.WriteAll(writer, b)
}

func ReadWelcome(reader io.Reader) (sessionID uint32, phase Phase, nextSeq uint64, err error) {
	sessionID, err = readUint32(reader)
	if err != nil {
		return 0, 0, 0, err
	}
	phaseByte, err := safeio.ReadAll(reader, 1)
	if err != nil {
		return 0, 0, 0, err
	}
	nextSeq, err = readUint64(reader)
	if err != nil {
		return 0, 0, 0, err
	}
	return sessionID, Phase(phaseByte[0]), nextSeq, nil
}

func WriteEndOfRecords(writer io.Writer, seq uint64, total uint32) error {
	var buf [13]byte
	b := wire.AppendUint8(buf[:0], uint8(EndOfRecords))
	b = wire.AppendUint64(b, seq)
	b = wire.AppendUint32(b, total)
	return safeio.WriteAll(writer, b)
}

func ReadEndOfRecords(reader io.Reader) (seq uint64, total uint32, err error) {
	seq, err = readUint64(reader)
	if err != nil {
		return 0, 0, err
	}
	total, err = readUint32(reader)
	if err != nil {
		return 0, 0, err
	}
	return seq, total, nil
}

func WriteQueryEOF(writer io.Writer, queryId uint8) error {
	return safeio.WriteAll(writer, []byte{uint8(QueryEOF), queryId})
}

func ReadQueryEOF(reader io.Reader) (uint8, error) {
	b, err := safeio.ReadAll(reader, 1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func deserializeAccountRecord(reader io.Reader) (account.Account, error) {
	bankName, err := readString(reader)
	if err != nil {
		return account.Account{}, err
	}
	bankId, err := readString(reader)
	if err != nil {
		return account.Account{}, err
	}
	accountNumber, err := readString(reader)
	if err != nil {
		return account.Account{}, err
	}
	entityId, err := readString(reader)
	if err != nil {
		return account.Account{}, err
	}
	entityName, err := readString(reader)
	if err != nil {
		return account.Account{}, err
	}
	return account.Account{
		BankName:      bankName,
		BankId:        bankId,
		AccountNumber: accountNumber,
		EntityId:      entityId,
		EntityName:    entityName,
	}, nil
}

func ReadAccountBatch(reader io.Reader) (uint64, []account.Account, error) {
	seq, err := readUint64(reader)
	if err != nil {
		return 0, nil, err
	}
	count, err := readCount(reader)
	if err != nil {
		return 0, nil, err
	}
	accounts := make([]account.Account, 0, count)
	for range count {
		acc, err := deserializeAccountRecord(reader)
		if err != nil {
			return 0, nil, err
		}
		accounts = append(accounts, acc)
	}
	return seq, accounts, nil
}

func ReadRawTransBatch(r io.Reader) (seq uint64, count uint16, payload []byte, err error) {
	seq, err = readUint64(r)
	if err != nil {
		return 0, 0, nil, err
	}
	countBytes, err := safeio.ReadAll(r, 2)
	if err != nil {
		return 0, 0, nil, err
	}
	count = binary.BigEndian.Uint16(countBytes)
	payloadSize, err := readUint32(r)
	if err != nil {
		return 0, 0, nil, err
	}
	payload, err = safeio.ReadAll(r, payloadSize)
	if err != nil {
		return 0, 0, nil, err
	}
	return seq, count, payload, nil
}

func ReadResultBatch(reader io.Reader) (queryID uint8, senderID uint8, seq uint64, results *queryresult.BatchResults, err error) {
	hdr, err := safeio.ReadAll(reader, 10)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	queryID = hdr[0]
	senderID = hdr[1]
	seq = binary.BigEndian.Uint64(hdr[2:])

	count, err := readCount(reader)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	results = &queryresult.BatchResults{}

	for range count {
		switch queryID {
		case 1:
			item, err := deserializeQuery1Result(reader)
			if err != nil {
				return 0, 0, 0, nil, err
			}
			results.Query1 = append(results.Query1, item)
		case 2:
			item, err := deserializeQuery2Result(reader)
			if err != nil {
				return 0, 0, 0, nil, err
			}
			results.Query2 = append(results.Query2, item)
		case 3:
			item, err := deserializeQuery3Result(reader)
			if err != nil {
				return 0, 0, 0, nil, err
			}
			results.Query3 = append(results.Query3, item)
		case 4:
			item, err := deserializeQuery4Result(reader)
			if err != nil {
				return 0, 0, 0, nil, err
			}
			results.Query4 = append(results.Query4, item)
		case 5:
			item, err := deserializeQuery5Result(reader)
			if err != nil {
				return 0, 0, 0, nil, err
			}
			results.Query5 = append(results.Query5, item)
		default:
			return 0, 0, 0, nil, fmt.Errorf("unexpected query id in result batch: %d", queryID)
		}
	}

	return queryID, senderID, seq, results, nil
}

func readString(r io.Reader) (string, error) {
	lenBytes, err := safeio.ReadAll(r, 2)
	if err != nil {
		return "", err
	}
	strLen := uint32(binary.BigEndian.Uint16(lenBytes))
	strBytes, err := safeio.ReadAll(r, strLen)
	if err != nil {
		return "", err
	}
	return string(strBytes), nil
}

func readCount(r io.Reader) (uint32, error) {
	b, err := safeio.ReadAll(r, 2)
	if err != nil {
		return 0, err
	}
	return uint32(binary.BigEndian.Uint16(b)), nil
}

func readUint32(r io.Reader) (uint32, error) {
	b, err := safeio.ReadAll(r, 4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func readUint64(r io.Reader) (uint64, error) {
	b, err := safeio.ReadAll(r, 8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func readFloat64(r io.Reader) (float64, error) {
	b, err := safeio.ReadAll(r, 8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
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
	amount, err := readFloat64(r)
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
	amount, err := readFloat64(r)
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
	paymentFormat, err := readString(r)
	if err != nil {
		return queryresult.Query3Result{}, err
	}
	amount, err := readFloat64(r)
	if err != nil {
		return queryresult.Query3Result{}, err
	}
	return queryresult.Query3Result{
		FromBank:      fromBank,
		FromAccount:   fromAccount,
		PaymentFormat: paymentFormat,
		Amount:        amount,
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
	return queryresult.Query4Result{BankId: bankId, AccountNumber: accountId}, nil
}

func deserializeQuery5Result(r io.Reader) (queryresult.Query5Result, error) {
	qty, err := readCount(r)
	if err != nil {
		return queryresult.Query5Result{}, err
	}
	return queryresult.Query5Result{Qty: qty}, nil
}

func serializeQuery1Result(dst []byte, r *queryresult.Query1Result) []byte {
	dst = wire.AppendString(dst, r.FromBank)
	dst = wire.AppendString(dst, r.FromAccount)
	dst = wire.AppendString(dst, r.ToBank)
	dst = wire.AppendString(dst, r.ToAccount)
	dst = wire.AppendFloat64(dst, r.Amount)
	return dst
}

func serializeQuery2Result(dst []byte, r *queryresult.Query2Result) []byte {
	dst = wire.AppendString(dst, r.BankName)
	dst = wire.AppendString(dst, r.FromBank)
	dst = wire.AppendString(dst, r.FromAccount)
	dst = wire.AppendFloat64(dst, r.Amount)
	return dst
}

func serializeQuery3Result(dst []byte, r *queryresult.Query3Result) []byte {
	dst = wire.AppendString(dst, r.FromBank)
	dst = wire.AppendString(dst, r.FromAccount)
	dst = wire.AppendString(dst, r.PaymentFormat)
	dst = wire.AppendFloat64(dst, r.Amount)
	return dst
}

func serializeQuery4Result(dst []byte, r *queryresult.Query4Result) []byte {
	dst = wire.AppendString(dst, r.BankId)
	dst = wire.AppendString(dst, r.AccountNumber)
	return dst
}

func serializeQuery5Result(dst []byte, r *queryresult.Query5Result) []byte {
	return wire.AppendUint16(dst, uint16(r.Qty))
}

func AppendQuery1Results(dst []byte, recs []queryresult.Query1Result) []byte {
	for i := range recs {
		dst = serializeQuery1Result(dst, &recs[i])
	}
	return dst
}

func AppendQuery2Results(dst []byte, recs []queryresult.Query2Result) []byte {
	for i := range recs {
		dst = serializeQuery2Result(dst, &recs[i])
	}
	return dst
}

func AppendQuery3Results(dst []byte, recs []queryresult.Query3Result) []byte {
	for i := range recs {
		dst = serializeQuery3Result(dst, &recs[i])
	}
	return dst
}

func AppendQuery4Results(dst []byte, recs []queryresult.Query4Result) []byte {
	for i := range recs {
		dst = serializeQuery4Result(dst, &recs[i])
	}
	return dst
}

func AppendQuery5Results(dst []byte, recs []queryresult.Query5Result) []byte {
	for i := range recs {
		dst = serializeQuery5Result(dst, &recs[i])
	}
	return dst
}
