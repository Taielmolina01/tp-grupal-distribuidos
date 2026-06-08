package daterangeandpaymentfilter

import (
	"slices"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func isValidPaymentMethod(t transfer.Transfer, config filter.FilterConfig) bool {
	return slices.Contains(config.PaymentFormats, t.PaymentFormat)
}

func CreateDateRangeAndPaymentMethod(config filter.FilterConfig) (worker.Worker, error) {
	return commonfilter.NewFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidPaymentMethod(t, config) && !t.Timestamp.Before(config.StartDateRange) && t.Timestamp.Before(config.EndDateRange)
		},
		transfer.ProjectForQ5Filter,
		records.TransferCodec,
		records.TransferForQ5FilterCodec,
	)
}
