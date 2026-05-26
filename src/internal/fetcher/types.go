package fetcher

import "tp-grupal-distribuidos/internal/common/middleware/newmiddleware"

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
	inputQueue       newmiddleware.Middleware
	outputQueues     []newmiddleware.Middleware
	queryId          uint8
	quote            string
	conversionsByDay map[string]map[string]float32
}

type apiResponseRates struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float32 `json:"rate"`
}
