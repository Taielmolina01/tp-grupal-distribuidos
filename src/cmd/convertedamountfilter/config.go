package main

import (
	"errors"
	"os"
	"strconv"

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
		Id:                   id,
		MomHost:              momHost,
		MomPort:              momPort,
		InputMiddlewarePrefix: inputMiddlewarePrefix,
		OutputQueue:          outputQueue,
		FilterAmount:         filterAmountInt,
		QueryID:              uint8(queryId),
	}

	if err := loadAmountVenv(&config); err != nil {
		return filter.FilterConfig{}, err
	}
	if err := loadQuoteVenv(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	return config, nil
}

// Helpers

func loadAmountVenv(config *filter.FilterConfig) error {
	amountStr := os.Getenv("AMOUNT")
	amount, err := strconv.ParseFloat(amountStr, 32)
	if err != nil {
		return errors.New("AMOUNT environment variable is required if FILTER_TYPE is AMOUNT or CONVERTED_AMOUNT_FILTER")
	}
	config.Amount = float64(amount)
	return nil
}

func loadQuoteVenv(config *filter.FilterConfig) error {
	quote := os.Getenv("QUOTE")
	if quote == "" {
		return errors.New("QUOTE environment variable is required if FILTER_TYPE is CONVERTED_AMOUNT_FILTER")
	}
	config.Quote = quote
	return nil
}
