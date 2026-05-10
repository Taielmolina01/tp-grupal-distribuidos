package filter

import (
	"slices"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
)

func isValidCurrency(t transfer.Transfer, config FilterConfig) bool {
	return slices.Contains(config.Currencies, t.PaymentCurrency)
}

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
