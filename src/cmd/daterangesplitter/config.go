package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/daterangesplitter"
)

func loadConfig() (daterangesplitter.DateRangeSplitterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputMiddlewarePrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputMiddlewarePrefix == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	avgOutputMiddlewarePrefix := os.Getenv("AVG_OUTPUT_MIDDLEWARE_PREFIX")
	if avgOutputMiddlewarePrefix == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("AVG_OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}
	avgOutputAmount, err := strconv.Atoi(os.Getenv("AVG_OUTPUT_AMOUNT"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("AVG_OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	filterOutputMiddlewarePrefix := os.Getenv("FILTER_OUTPUT_MIDDLEWARE_PREFIX")
	if filterOutputMiddlewarePrefix == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("FILTER_OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}
	filterOutputAmount, err := strconv.Atoi(os.Getenv("FILTER_OUTPUT_AMOUNT"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("FILTER_OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}
	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}
	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}

	return daterangesplitter.DateRangeSplitterConfig{
		Id:                           id,
		ExpectedEOFs:                 expectedEOFs,
		MomHost:                      momHost,
		MomPort:                      momPort,
		InputMiddlewarePrefix:        inputMiddlewarePrefix,
		AvgOutputMiddlewarePrefix:    avgOutputMiddlewarePrefix,
		AvgOutputAmount:              avgOutputAmount,
		FilterOutputMiddlewarePrefix: filterOutputMiddlewarePrefix,
		FilterOutputAmount:           filterOutputAmount,
		QueryID:                      uint8(queryID),
		AvgPeriodStart:               time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC),
		AvgPeriodEnd:                 time.Date(2022, 9, 5, 23, 59, 59, 0, time.UTC),
		FilterPeriodStart:            time.Date(2022, 9, 6, 0, 0, 0, 0, time.UTC),
		FilterPeriodEnd:              time.Date(2022, 9, 15, 23, 59, 59, 0, time.UTC),
		PersistPath:                  persistPath,
		PersistBatchSize:             persistBatchSize,
		PersistFlushInterval:         persistFlushInterval,
	}, nil
}
