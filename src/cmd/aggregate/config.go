package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/aggregate"
	commonconfig "tp-grupal-distribuidos/internal/common/config"
)

func loadConfig() (aggregate.AggregateConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregate.AggregateConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputPrefix == "" {
		return aggregate.AggregateConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputPrefix := os.Getenv("OUTPUT_MIDDLEWARE_PREFIX")
	if outputPrefix == "" {
		return aggregate.AggregateConfig{}, errors.New("OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputAmount, err := strconv.Atoi(os.Getenv("OUTPUT_AMOUNT"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	maxBatchSize, err := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("MAX_BATCH_SIZE environment variable is required and must be a number")
	}

	maxBatchBytes, err := commonconfig.ParseMaxBatchBytes()
	if err != nil {
		return aggregate.AggregateConfig{}, err
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return aggregate.AggregateConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}

	return aggregate.AggregateConfig{
		Id:                     id,
		ExpectedEOFs:           expectedEOFs,
		MomHost:                momHost,
		MomPort:                momPort,
		QueryID:                uint8(queryID),
		InputMiddlewarePrefix:  inputPrefix,
		OutputMiddlewarePrefix: outputPrefix,
		OutputAmount:           outputAmount,
		MaxBatchSize:           maxBatchSize,
		MaxBatchBytes:          maxBatchBytes,
		PersistPath:            persistPath,
		PersistBatchSize:       persistBatchSize,
		PersistFlushInterval:   persistFlushInterval,
	}, nil
}
