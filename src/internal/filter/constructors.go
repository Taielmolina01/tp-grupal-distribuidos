package filter

import (
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/filterandsplitter"
)

const IGNORED_CURRENCY = "Bitcoin"

func CreateCurrencyFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config)
		},
		transfer.ProjectAfterCurrency,
		records.TransferCodec,
		records.TransferAfterCurrencyCodec,
	)
}

func CreateAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
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
		records.TransferAfterCurrencyCodec,
		records.Query1ResultCodec,
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
		records.TransferCodec,
		records.TransferCodec,
	)
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return t.PaymentCurrency != IGNORED_CURRENCY && isValidPaymentMethod(t, config) && !t.Timestamp.Before(config.StartDateRange) && t.Timestamp.Before(config.EndDateRange)
		},
		transfer.ProjectForQ5Filter,
		records.TransferCodec,
		records.TransferForQ5FilterCodec,
	)
}

func CreateFilterAndSplitter(config filterandsplitter.FilterAndSplitterConfig) (worker.Worker, error) {
	return filterandsplitter.NewFilterAndSplitter(
		config,
	)
}

func CreateConvertedAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
	)
}
