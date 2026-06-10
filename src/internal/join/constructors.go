package join

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateTransferAccountByBankJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = queryresult.Query2ID
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
			} else if t1.AmountPaid == t2.AmountPaid {
				if t1.FromBankAccount > t2.FromBankAccount {
					return t1
				} else {
					return t2
				}
			} else {
				return t2
			}
		},
		records.TransferForQ2Codec,
		records.AccountCodec,
		records.Query2ResultCodec,
	)
}
