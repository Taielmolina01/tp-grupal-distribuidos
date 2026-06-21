package currencyfilter

import (
	"log/slog"
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

	clusters := make([]commonfilter.OutputCluster, 0, len(config.OutputClusters))
	for _, c := range config.OutputClusters {
		m, err := newmiddleware.NewShardedMiddleware(connSettings, c.Prefix, "", "")
		if err != nil {
			for _, cl := range clusters {
				if closeErr := cl.Middleware.Close(); closeErr != nil {
					slog.Error("While closing cluster middleware after creation failure", "err", closeErr)
				}
			}
			return nil, err
		}
		clusters = append(clusters, commonfilter.OutputCluster{
			Middleware: m,
			Hasher:     shard.New(c.NodeCount),
		})
	}

	return commonfilter.NewFilter(
		config,
		func(t transfer.Transfer) bool { return isValidCurrency(t, config) },
		transfer.ProjectAfterCurrency,
		func(t transfer.TransferAfterCurrency) []string {
			return []string{strconv.FormatFloat(t.AmountPaid, 'f', -1, 64)}
		},
		records.TransferCodec,
		records.TransferAfterCurrencyCodec,
		clusters,
	)
}
