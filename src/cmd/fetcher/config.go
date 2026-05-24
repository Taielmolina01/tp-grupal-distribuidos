package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/fetcher"
)

const QUEUES_SEPARATOR = ","

func loadConfig() (fetcher.FetcherConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return fetcher.FetcherConfig{}, err
	}

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

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return fetcher.FetcherConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	outputQueuesList := splitter.Split(outputQueues, QUEUES_SEPARATOR)

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
		Id:           id,
		MomHost:      momHost,
		MomPort:      momPort,
		InputQueue:   inputQueue,
		Quote:        quote,
		OutputQueues: outputQueuesList,
		QueryId:      uint8(queryId),
	}, nil
}
