package tcpproto

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/transfer"
)

func marshalTransRecord(dst []byte, t *transfer.Transfer) []byte {
	dst = wire.AppendInt64(dst, t.Timestamp.Unix())
	dst = wire.AppendString(dst, t.FromBank)
	dst = wire.AppendString(dst, t.FromBankAccount)
	dst = wire.AppendString(dst, t.ToBank)
	dst = wire.AppendString(dst, t.ToBankAccount)
	dst = wire.AppendFloat64(dst, t.AmountReceived)
	dst = wire.AppendString(dst, t.ReceivingCurrency)
	dst = wire.AppendFloat64(dst, t.AmountPaid)
	dst = wire.AppendString(dst, t.PaymentCurrency)
	dst = wire.AppendString(dst, t.PaymentFormat)
	dst = wire.AppendBool(dst, t.IsLaundering)
	return dst
}

func marshalAccountRecord(dst []byte, a *account.Account) []byte {
	dst = wire.AppendString(dst, a.BankName)
	dst = wire.AppendString(dst, a.BankId)
	dst = wire.AppendString(dst, a.AccountNumber)
	dst = wire.AppendString(dst, a.EntityId)
	dst = wire.AppendString(dst, a.EntityName)
	return dst
}
