package fetcher

import (
	"tp-grupal-distribuidos/internal/common/middleware"
)

type FetcherConfig struct {
	MomHost          string
	MomPort          int
	InputQueue       string
	InputExchange    string
	InputRoutingKeys []string
	OutputQueue      string
	Quote            string
	QueryId          uint8
}

type Fetcher struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	queryId     uint8
	quote       string
	ratesCache  map[string]float64
	forwarded   uint32
}

type apiResponseRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}
