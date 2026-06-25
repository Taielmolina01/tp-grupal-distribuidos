package main

import (
	"errors"
	"os"
	"strconv"

	commonconfig "tp-grupal-distribuidos/internal/common/config"
	"tp-grupal-distribuidos/internal/fetcher"
)

func loadConfig() (fetcher.FetcherConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return fetcher.FetcherConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputMiddlewarePrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputMiddlewarePrefix == "" {
		return fetcher.FetcherConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	idStr := os.Getenv("ID")
	if idStr == "" {
		return fetcher.FetcherConfig{}, errors.New("ID environment variable is required")
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("ID environment variable must be a number")
	}

	outputMiddleware := os.Getenv("OUTPUT_MIDDLEWARE_PREFIX")
	if outputMiddleware == "" {
		return fetcher.FetcherConfig{}, errors.New("OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputAmountStr := os.Getenv("OUTPUT_AMOUNT")
	if outputAmountStr == "" {
		return fetcher.FetcherConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required")
	}
	outputAmount, err := strconv.Atoi(outputAmountStr)
	if err != nil {
		return fetcher.FetcherConfig{}, errors.New("OUTPUT_AMOUNT environment variable must be a number")
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

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return fetcher.FetcherConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	senderTrackerCapacity, err := commonconfig.ParseSenderTrackerCapacity()
	if err != nil {
		return fetcher.FetcherConfig{}, err
	}

	return fetcher.FetcherConfig{
		MomHost:                momHost,
		MomPort:                momPort,
		InputMiddlewarePrefix:  inputMiddlewarePrefix,
		Id:                     id,
		Quote:                  quote,
		OutputMiddlewarePrefix: outputMiddleware,
		OutputAmount:           outputAmount,
		ExpectedInputSenders:   inputSenders,
		QueryID:                uint8(queryId),
		PersistPath:            persistPath,
		SenderTrackerCapacity:  senderTrackerCapacity,
	}, nil
}
