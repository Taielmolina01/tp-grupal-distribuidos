package filter

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/account"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
)

func CreateCurrencyFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return isValidCurrency(t, config)
	})
}

func CreateAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.AmountPaid > config.Amount
	})
}

func CreateDateRangeFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
	})
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (worker.Worker, error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return isValidCurrency(t, config) && t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
	})
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
	return newDistinctFilter(config, func(t1 transfer.Transfer, t2 transfer.Transfer) bool {
		return t1.Equals(t2)
	})
}

func CreateFilterByAccountId(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(config, func(ac1 account.Account, ac2 account.Account) bool {
		return ac1.Equals(ac2)
	})
}

func CreateAverageFilter(config FilterConfig) (worker.Worker, error) {
	// Precondiciones:
	// - Primer número: monto de la transferencia cruda
	// - Segundo número: monto promedio de las transferencias
	return newAverageFilter(config, func(n1 float32, n2 float32) bool {
		return n1 <= n2+config.Amount && n1 >= n2-config.Amount
	})
}
