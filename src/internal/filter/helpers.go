package filter

import (
	"slices"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/transfer"
)

func isValidCurrency(t transfer.Transfer, config FilterConfig) bool {
	return slices.Contains(config.Currencies, t.PaymentCurrency)
}
