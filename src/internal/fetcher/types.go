package fetcher

import "tp-grupal-distribuidos/internal/common/middleware"

type FetcherConfig struct {
	Id           int
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQueues []string
	Quote        string
	QueryId      uint8
}

type Fetcher struct {
	inputQueue       middleware.Middleware
	outputQueues     []middleware.Middleware
	queryId          uint8
	quote            string
	conversionsByDay map[string]map[string]float64
}

type apiResponse struct {
	Rates []apiResponseRates
}

type apiResponseRates struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}
