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
	OutputQueues     []string
	Quote            string
	QueryId          uint8
}

type Fetcher struct {
	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	queryId      uint8
	quote        string
	actualIndex  int
	ratesCache   map[string]float64
}

type apiResponseRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}
