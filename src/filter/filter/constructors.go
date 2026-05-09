package filter

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/transfer"
)

func CreateCurrencyFilter(config FilterConfig) (*Filter[transfer.Transfer], error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.PaymentCurrency == config.Currency
	})
}

func CreateAmountFilter(config FilterConfig) (*Filter[transfer.Transfer], error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.PaymentCurrency > config.Amount
	})
}

func CreateDateRangeFilter(config FilterConfig) (*Filter[transfer.Transfer], error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		return t.Timestamp < config.EndDateRange && t.Timestamp > config.StartDateRange
	})
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (*Filter[transfer.Transfer], error) {
	return newFilter(config, func(t transfer.Transfer) bool {
		found := false
		for _, currency := range config.Currencies {
			if currency == t.PaymentCurrency {
				found = true
			}
		}
		return t.Timestamp < config.EndDateRange && t.Timestamp > config.StartDateRange && found
	})
}
