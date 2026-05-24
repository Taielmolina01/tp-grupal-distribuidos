package fetcher

import (
	"tp-grupal-distribuidos/internal/common/worker"
)

func CreateFetcher(config FetcherConfig) (worker.Worker, error) {
	return createFetcherImpl(config)
}
