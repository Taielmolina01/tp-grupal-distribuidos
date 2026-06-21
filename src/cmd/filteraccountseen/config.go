package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/filter/filteraccountseen"
)

func loadConfig() (filteraccountseen.FilterAccountSeenConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	outputMiddleware := os.Getenv("OUTPUT_MIDDLEWARE")
	if outputMiddleware == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("OUTPUT_MIDDLEWARE environment variable is required")
	}

	inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputPrefix == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	maxBatchSize, err := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("MAX_BATCH_SIZE environment variable is required and must be a number")
	}

	maxBatchBytes, err := strconv.Atoi(os.Getenv("MAX_BATCH_BYTES"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("MAX_BATCH_BYTES environment variable is required and must be a number")
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required")
	}

	return filteraccountseen.FilterAccountSeenConfig{
		Id:                    id,
		ExpectedEOFs:          expectedEOFs,
		OutputMiddleware:      outputMiddleware,
		MomHost:               momHost,
		MomPort:               momPort,
		InputMiddlewarePrefix: inputPrefix,
		QueryID:               queryID,
		MaxBatchSize:          maxBatchSize,
		MaxBatchBytes:         maxBatchBytes,
		PersistPath:           persistPath,
		PersistBatchSize:      persistBatchSize,
		PersistFlushInterval:  persistFlushInterval,
	}, nil
}
