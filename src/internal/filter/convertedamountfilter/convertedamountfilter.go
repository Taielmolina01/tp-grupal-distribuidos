package convertedamountfilter

import (
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commonfilter"
)

func CreateConvertedAmountFilter(config filter.FilterConfig) (worker.Worker, error) {
	connSettings := newmiddleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	outputMiddleware, err := newmiddleware.NewQueueMiddleware(connSettings, config.OutputQueue)
	if err != nil {
		return nil, err
	}
	clusters := []newmiddleware.ShardedCluster{{
		Middleware: outputMiddleware,
		Hasher:     shard.New(1),
	}}
	return commonfilter.NewFilter(
		config,
		func(t fetcherresponse.FetcherResponse) bool {
			return t.ConvertedAmount < config.Amount
		},
		func(fetcherresponse.FetcherResponse) transfer.FinalTransferForQ5 {
			return transfer.ProjectForQ5Final()
		},
		func(transfer.FinalTransferForQ5, uint64) []string { return nil },
		records.FetcherResponseCodec,
		records.FinalTransferForQ5Codec,
		clusters,
	)
}
