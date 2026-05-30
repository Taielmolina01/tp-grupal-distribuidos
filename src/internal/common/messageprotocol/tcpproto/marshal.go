package tcpproto

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/byteconv"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func marshalTransRecord(dst []byte, t *transfer.Transfer) []byte {
	dst = byteconv.AppendInt64(dst, t.Timestamp.Unix())
	dst = byteconv.AppendString(dst, t.FromBank)
	dst = byteconv.AppendString(dst, t.FromBankAccount)
	dst = byteconv.AppendString(dst, t.ToBank)
	dst = byteconv.AppendString(dst, t.ToBankAccount)
	dst = byteconv.AppendFloat64(dst, t.AmountReceived)
	dst = byteconv.AppendString(dst, t.ReceivingCurrency)
	dst = byteconv.AppendFloat64(dst, t.AmountPaid)
	dst = byteconv.AppendString(dst, t.PaymentCurrency)
	dst = byteconv.AppendString(dst, t.PaymentFormat)
	dst = byteconv.AppendBool(dst, t.IsLaundering)
	return dst
}

func marshalAccountRecord(dst []byte, a *account.Account) []byte {
	dst = byteconv.AppendString(dst, a.BankName)
	dst = byteconv.AppendString(dst, a.BankId)
	dst = byteconv.AppendString(dst, a.AccountNumber)
	dst = byteconv.AppendString(dst, a.EntityId)
	dst = byteconv.AppendString(dst, a.EntityName)
	return dst
}
