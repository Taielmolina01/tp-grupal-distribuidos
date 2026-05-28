package join

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateTransferAccountByBankJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = inner.Query2ID
	return newTwoInputJoin(
		config,
		func(t transfer.TransferForQ2) string { return normalizer.NormalizeBankID(t.FromBank) },
		func(a account.Account) string { return normalizer.NormalizeBankID(a.BankId) },
		func(t transfer.TransferForQ2, a account.Account) queryresult.Query2Result {
			return queryresult.Query2Result{
				BankName:    a.BankName,
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				Amount:      t.AmountPaid,
			}
		},
		func(t1, t2 transfer.TransferForQ2) transfer.TransferForQ2 {
			if t1.AmountPaid > t2.AmountPaid {
				return t1
			}
			return t2
		},
	)
}
