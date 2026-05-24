package fetcher

import "tp-grupal-distribuidos/internal/common/middleware"

type FetcherConfig struct {
	Id             int
	MomHost        string
	MomPort        int
	InputQueue     string
	OutputExchange string
	OutputQueues   []string
	QueryId        uint8
}

type Fetcher struct {
	InputQueue   middleware.Middleware
	OutputQueues []middleware.Middleware
	QueryId      uint8
}
