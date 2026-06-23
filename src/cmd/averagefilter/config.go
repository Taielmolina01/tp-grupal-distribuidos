package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/averagefilter"
	commonconfig "tp-grupal-distribuidos/internal/common/config"
	"tp-grupal-distribuidos/internal/common/queryresult"
)

func loadConfig() (averagefilter.AverageFilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputTransfersPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputTransfersPrefix == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	inputAvgsPrefix := os.Getenv("AVG_INPUT_MIDDLEWARE_PREFIX")
	if inputAvgsPrefix == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("AVG_INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	expectedTransfersEofs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	expectedAvgEofs, err := strconv.Atoi(os.Getenv("AVG_EXPECTED_EOFS"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("AVG_EXPECTED_EOFS environment variable is required and must be a number")
	}

	maxBatchSize, err := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("MAX_BATCH_SIZE environment variable is required and must be a number")
	}

	maxBatchBytes, err := commonconfig.ParseMaxBatchBytes()
	if err != nil {
		return averagefilter.AverageFilterConfig{}, err
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required and must be a valid duration")
	}

	return averagefilter.AverageFilterConfig{
		Id:                             id,
		MomHost:                        momHost,
		MomPort:                        momPort,
		InputTransfersMiddlewarePrefix: inputTransfersPrefix,
		InputAvgsMiddlewarePrefix:      inputAvgsPrefix,
		OutputQueue:                    outputQueue,
		ExpectedTransfersEofs:          expectedTransfersEofs,
		ExpectedAvgEofs:                expectedAvgEofs,
		MaxBatchSize:                   maxBatchSize,
		MaxBatchBytes:                  maxBatchBytes,
		PersistPath:                    persistPath,
		PersistBatchSize:               persistBatchSize,
		PersistFlushInterval:           persistFlushInterval,
		QueryID:                        queryresult.Query3ID,
	}, nil
}
