package amountfilter

import (
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func CreateAmountFilter(config filter.FilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	outputMiddleware, err := newmiddleware.NewQueueMiddleware(connSettings, config.OutputQueue)
	if err != nil {
		return nil, err
	}
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
		func(_ int, _ queryresult.Query1Result) []string { return []string{""} },
		records.TransferAfterCurrencyCodec,
		records.Query1ResultCodec,
		outputMiddleware,
		shard.QueueRouter{},
	)
}
