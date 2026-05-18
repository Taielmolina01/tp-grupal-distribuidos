package join

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateTransferAccountByBankJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = inner.Query2ID
	return newTwoInputJoin(
		config,
		func(t transfer.Transfer) string { return t.FromBank },
		func(a account.Account) string { return a.BankId },
		func(t transfer.Transfer, a account.Account) queryresult.Query2Result {
			return queryresult.Query2Result{
				BankName:    a.BankName,
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				Amount:      t.AmountPaid,
			}
		},
	)
}
