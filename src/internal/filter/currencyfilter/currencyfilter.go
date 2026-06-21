package currencyfilter

import (
	"slices"
	"strconv"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func isValidCurrency(t transfer.Transfer, config filter.FilterConfig) bool {
	return slices.Contains(config.Currencies, t.PaymentCurrency)
}

func CreateCurrencyFilter(config filter.FilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	outputMiddleware, err := newmiddleware.NewShardedMiddleware(connSettings, config.OutputExchange, "", "")
	if err != nil {
		return nil, err
	}
	router := shard.NewMultiCluster(config.OutputClusters)
	return commonfilter.NewFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config)
		},
		transfer.ProjectAfterCurrency,
		func(clientID int, t transfer.TransferAfterCurrency) []string {
			return router.RoutingKeysFor(clientID, strconv.FormatFloat(t.AmountPaid, 'f', -1, 64))
		},
		records.TransferCodec,
		records.TransferAfterCurrencyCodec,
		outputMiddleware,
		router,
	)
}
