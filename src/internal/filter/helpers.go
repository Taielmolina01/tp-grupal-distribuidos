package filter

import (
	"slices"

	"tp-grupal-distribuidos/internal/common/transfer"
)

func isValidCurrency(t transfer.Transfer, config FilterConfig) bool {
	return slices.Contains(config.Currencies, t.PaymentCurrency)
}

func isValidPaymentMethod(t transfer.Transfer, config FilterConfig) bool {
	return slices.Contains(config.PaymentFormats, t.PaymentFormat)
}
