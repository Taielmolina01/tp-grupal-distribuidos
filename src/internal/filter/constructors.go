package filter

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateCurrencyFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
		1,
	)
}

func CreateAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return t.AmountPaid > config.Amount
		},
		func(t transfer.Transfer) queryresult.Query1Result {
			return queryresult.Query1Result{
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				ToBank:      t.ToBank,
				ToAccount:   t.ToBankAccount,
				Amount:      t.AmountPaid,
			}
		},
		1,
	)
}

func CreateDateRangeFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
		1,
	)
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config) && t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
		1,
	)
}

func CreateCountAndFilter(config CountAndFilterConfig) (worker.Worker, error) {
	return newCountAndFilter[transfer.Transfer](config)
}

func CreateFilterAndSplitter(config FilterConfig) (worker.Worker, error) {
	return newFilterAndSplitter(
		config,
		func(t transfer.Transfer) bool {
			return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) (t1 transfer.SplittedTransfer, t2 transfer.SplittedTransfer) {
			// TO DO: rellenar esto
			return transfer.SplittedTransfer{}, transfer.SplittedTransfer{}
		},
	)
}

func CreateTransferDistinctFilter(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(
		config,
		func(t1 transfer.Transfer, t2 transfer.Transfer) bool {
			return t1.Equals(t2)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
		func(t transfer.Transfer) string {
			return t.FromBank
		},
	)
}

func CreateBankDistinctFilter(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(
		config,
		func(ac1 account.Account, ac2 account.Account) bool {
			return ac1.BankId == ac2.BankId
		},
		func(ac account.Account) string {
			return ac.BankId
		},
		func(t account.Account) string {
			return normalizer.NormalizeBankID(t.BankId)
		},
	)
}

func CreateAverageFilter(config FilterConfig) (worker.Worker, error) {
	return newAverageFilter(
		config,
		func(transferAmount float32, avg float32) bool {
			return transferAmount < avg/100
		},
		3,
	)
}
