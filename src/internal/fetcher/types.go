package fetcher

import (
	"time"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/priorityqueue"
)

type FetcherConfig struct {
	MomHost          string
	MomPort          int
	InputQueue       string
	InputExchange    string
	InputRoutingKeys []string
	OutputQueue      string
	Quote            string
	QueryID          uint8
}

type Fetcher struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	queryId        uint8
	quote          string
	ratesCache     map[mapTypeDTO]float64
	forwarded      uint32
	ratesCacheHeap priorityqueue.PriorityQueue[heapDTO]
}

type heapDTO struct {
	time     time.Time
	cacheKey mapTypeDTO
}

type mapTypeDTO struct {
	time time.Time
	base string
}

type apiResponseRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}
