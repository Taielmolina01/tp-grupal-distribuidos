package join

import (
	"strings"

	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func normalizeBankID(s string) string {
	t := strings.TrimLeft(s, "0")
	if t == "" {
		return "0"
	}
	return t
}

func CreateSplittedTransferJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = inner.Query4ID
	return newSingleInputJoin(
		config,
		func(t transfer.SplittedTransfer) bool { return t.IsLeftPart },
		func(t transfer.SplittedTransfer) string { return t.Transfer.ToBankAccount },
		func(t transfer.SplittedTransfer) string { return t.Transfer.FromBankAccount },
		func(left, right transfer.SplittedTransfer) queryresult.Query4Result {
			return queryresult.Query4Result{
				BankId:    left.Transfer.FromBank,
				AccountId: left.Transfer.FromBankAccount,
			}
		},
	)
}

func CreateTransferAccountByBankJoin(config JoinConfig) (worker.Worker, error) {
	config.QueryID = inner.Query2ID
	return newTwoInputJoin(
		config,
		func(t transfer.Transfer) string { return normalizeBankID(t.FromBank) },
		func(a account.Account) string { return normalizeBankID(a.BankId) },
		func(t transfer.Transfer, a account.Account) queryresult.Query2Result {
			return queryresult.Query2Result{
				BankName:    a.BankName,
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				Amount:      t.AmountPaid,
			}
		},
		func(t1, t2 transfer.Transfer) transfer.Transfer {
			if t1.AmountPaid > t2.AmountPaid {
				return t1
			}
			return t2
		},
	)
}
