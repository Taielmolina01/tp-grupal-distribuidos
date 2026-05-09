package filter

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/worker"
)

func CreateCurrencyFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.PaymentCurrency == config.Currency
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
		found := false
		for _, currency := range config.Currencies {
			if currency == t.PaymentCurrency {
				found = true
			}
		}
		return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange) && found
	})
}
