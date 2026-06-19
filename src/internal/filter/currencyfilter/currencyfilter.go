package currencyfilter

import (
	"slices"
	"strconv"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func isValidCurrency(t transfer.Transfer, config filter.FilterConfig) bool {
	return slices.Contains(config.Currencies, t.PaymentCurrency)
}

func CreateCurrencyFilter(config filter.FilterConfig) (worker.Worker, error) {
	return commonfilter.NewFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config)
		},
		transfer.ProjectAfterCurrency,
		func(t transfer.TransferAfterCurrency) []string {
			return []string{strconv.FormatFloat(t.AmountPaid, 'f', -1, 64)}
		},
		records.TransferCodec,
		records.TransferAfterCurrencyCodec,
	)
}
