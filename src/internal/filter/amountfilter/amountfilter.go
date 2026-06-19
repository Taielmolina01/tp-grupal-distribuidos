package amountfilter

import (
	"strconv"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func CreateAmountFilter(config filter.FilterConfig) (worker.Worker, error) {
	return commonfilter.NewFilter(
		config,
		func(t transfer.TransferAfterCurrency) bool {
			return t.AmountPaid < config.Amount
		},
		func(t transfer.TransferAfterCurrency) queryresult.Query1Result {
			return queryresult.Query1Result{
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				ToBank:      t.ToBank,
				ToAccount:   t.ToBankAccount,
				Amount:      t.AmountPaid,
			}
		},
		func(q queryresult.Query1Result) []string {
			return []string{strconv.FormatFloat(q.Amount, 'f', -1, 64)}
		},
		records.TransferAfterCurrencyCodec,
		records.Query1ResultCodec,
	)
}
