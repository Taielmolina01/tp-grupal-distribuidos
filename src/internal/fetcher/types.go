package fetcher

import (
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
	"tp-grupal-distribuidos/internal/common/outputtracker"
	"tp-grupal-distribuidos/internal/common/sendertracker"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/statemap"
)

type FetcherConfig struct {
	MomHost              string
	MomPort              int
	InputQueue           string
	InputExchange        string
	InputRoutingKeys     []string
	OutputClusters       []shard.ClusterConfig
	ExpectedInputSenders int
	Quote                string
	QueryID              uint8
}

type outputCluster struct {
	middleware newmiddleware.Middleware
	hasher     shard.Hasher
}

type Fetcher struct {
	inputQueue       middleware.Middleware
	outputClusters   []outputCluster
	queryId          uint8
	quote            string
	ratesCache       map[string]float64
	states           statemap.StateMap[clientState]
	expectedSenders  int
}

type apiResponseRate struct {
	Date  string  `json:"date"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}

type clientState struct {
	tracker       *sendertracker.SenderTracker
	outputTracker *outputtracker.OutputTracker
}
