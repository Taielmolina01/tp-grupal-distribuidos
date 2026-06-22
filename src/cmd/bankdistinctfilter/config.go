package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

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

	inputQueue := os.Getenv("INPUT_QUEUE")

	outputQueue := os.Getenv("OUTPUT_QUEUE")

	filterAmount := os.Getenv("FILTER_AMOUNT")
	if filterAmount == "" {
		return filter.FilterConfig{}, errors.New("FILTER_AMOUNT environment variable is required")
	}

	filterAmountInt, err := strconv.Atoi(filterAmount)
	if err != nil {
		return filter.FilterConfig{}, errors.New("FILTER_AMOUNT environment variable must be a number")
	}

	queryIdStr := os.Getenv("QUERY_ID")
	if queryIdStr == "" {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required")
	}
	queryId, err := strconv.Atoi(queryIdStr)
	if err != nil {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	config := filter.FilterConfig{
		Id:           id,
		MomHost:      momHost,
		MomPort:      momPort,
		InputQueue:   inputQueue,
		OutputQueue:  outputQueue,
		FilterAmount: filterAmountInt,
		QueryID:      uint8(queryId),
	}

	if err := loadBankDistinctVenv(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	return config, nil
}

// Helpers

func loadBankDistinctVenv(config *filter.FilterConfig) error {
	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return errors.New("OUTPUT_QUEUES environment variable is required")
	}
	config.OutputQueues = strings.Split(outputQueues, ",")
	return nil
}
