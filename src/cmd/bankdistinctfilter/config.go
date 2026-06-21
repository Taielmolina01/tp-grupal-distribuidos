package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/splitter"
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

	outputRoutingKeysStr := os.Getenv("OUTPUT_ROUTING_KEYS")
	outputRoutingKeys := []string{}
	if outputRoutingKeysStr != "" {
		outputRoutingKeys = strings.Split(outputRoutingKeysStr, ",")
	}

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

	rightInputExchange := os.Getenv("RIGHT_INPUT_EXCHANGE")
	rightInputQueue := os.Getenv("RIGHT_INPUT_QUEUE")
	leftInputQueue := os.Getenv("LEFT_INPUT_QUEUE")
	rightInputRoutingKeysStr := os.Getenv("RIGHT_INPUT_ROUTING_KEYS")
	rightInputRoutingKeys := []string{}
	if rightInputRoutingKeysStr != "" {
		rightInputRoutingKeys = splitter.Split(rightInputRoutingKeysStr, ",")
	}

	config := filter.FilterConfig{
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputQueue:            inputQueue,
		OutputQueue:           outputQueue,
		OutputRoutingKeys:     outputRoutingKeys,
		LeftInputQueue:        leftInputQueue,
		RightInputQueue:       rightInputQueue,
		FilterAmount:          filterAmountInt,
		QueryId:               uint8(queryId),
		RightInputExchange:    rightInputExchange,
		RightInputRoutingKeys: rightInputRoutingKeys,
	}

	if err := loadFilterTypeConfig(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	return config, nil
}

// Helpers

func loadBankDistinctVenv(config *filter.FilterConfig) error {
	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return errors.New("OUTPUT_QUEUES environment variable is required if FILTER_TYPE is BANK_DISTINCT")
	}
	config.OutputQueues = strings.Split(outputQueues, ",")
	return nil
}

func loadFilterTypeConfig(config *filter.FilterConfig) error {
	filterTypeVenv := os.Getenv("FILTER_TYPE")
	if filterTypeVenv == "" {
		return errors.New("FILTER_TYPE environment variable is required")
	}

	config.Type = filter.FilterType(filterTypeVenv)

	if err := loadBankDistinctVenv(config); err != nil {
		return err
	}

	return nil
}
