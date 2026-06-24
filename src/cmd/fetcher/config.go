package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/fetcher"
)

const QUEUES_SEPARATOR = ","

func loadConfig() (fetcher.FetcherConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return fetcher.FetcherConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return fetcher.FetcherConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return fetcher.FetcherConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	inputRoutingKeys := []string{}
	if inputRoutingKeysStr := os.Getenv("INPUT_ROUTING_KEYS"); inputRoutingKeysStr != "" {
		inputRoutingKeys = splitter.Split(inputRoutingKeysStr, QUEUES_SEPARATOR)
	}

	var outputClusters []shard.ClusterConfig
	if clustersStr := os.Getenv("OUTPUT_CLUSTERS"); clustersStr == "" {
		return fetcher.FetcherConfig{}, errors.New("OUTPUT_CLUSTERS environment variable is required")
	} else {
		for _, part := range strings.Split(clustersStr, ",") {
			prefixAndCount := strings.SplitN(part, ":", 2)
			if len(prefixAndCount) != 2 {
				return fetcher.FetcherConfig{}, errors.New("invalid OUTPUT_CLUSTERS format, expected prefix:count,prefix:count")
			}
			count, err := strconv.Atoi(prefixAndCount[1])
			if err != nil {
				return fetcher.FetcherConfig{}, errors.New("invalid OUTPUT_CLUSTERS node count")
			}
			outputClusters = append(outputClusters, shard.ClusterConfig{
				Prefix:    prefixAndCount[0],
				NodeCount: count,
			})
		}
	}

	inputSendersStr := os.Getenv("INPUT_SENDERS")
	if inputSendersStr == "" {
		return fetcher.FetcherConfig{}, errors.New("INPUT_SENDERS environment variable is required")
	}
	inputSenders, err := strconv.Atoi(inputSendersStr)
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("INPUT_SENDERS environment variable must be a number")
	}

	queryIdStr := os.Getenv("QUERY_ID")
	if queryIdStr == "" {
		return fetcher.FetcherConfig{}, errors.New("QUERY_ID environment variable is required")
	}
	queryId, err := strconv.Atoi(queryIdStr)
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	quote := os.Getenv("QUOTE")
	if quote == "" {
		return fetcher.FetcherConfig{}, errors.New("QUOTE environment variable is required")
	}

	return fetcher.FetcherConfig{
		MomHost:              momHost,
		MomPort:              momPort,
		InputQueue:           inputQueue,
		InputExchange:        inputExchange,
		InputRoutingKeys:     inputRoutingKeys,
		Quote:                quote,
		OutputClusters:       outputClusters,
		ExpectedInputSenders: inputSenders,
		QueryID:              uint8(queryId),
	}, nil
}
