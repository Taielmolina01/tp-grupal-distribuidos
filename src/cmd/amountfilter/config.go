package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/common/filter"
)

func loadConfig() (filter.FilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filter.FilterConfig{}, err
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputMiddlewarePrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputMiddlewarePrefix == "" {
		return filter.FilterConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return filter.FilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	filterAmountInt, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	queryId, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	config := filter.FilterConfig{
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputMiddlewarePrefix: inputMiddlewarePrefix,
		OutputQueue:           outputQueue,
		FilterAmount:          filterAmountInt,
		QueryID:               uint8(queryId),
	}

	if err := loadAmountVenv(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return filter.FilterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}
	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}
	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}
	config.PersistPath = persistPath
	config.PersistBatchSize = persistBatchSize
	config.PersistFlushInterval = persistFlushInterval

	return config, nil
}

func loadAmountVenv(config *filter.FilterConfig) error {
	amount, err := strconv.ParseFloat(os.Getenv("AMOUNT"), 64)
	if err != nil {
		return errors.New("AMOUNT environment variable is required")
	}
	config.Amount = amount
	return nil
}
