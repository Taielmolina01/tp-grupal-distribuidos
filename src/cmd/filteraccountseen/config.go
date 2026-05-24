package main

import (
	"errors"
	"os"
	"strconv"

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

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	inputPrefix := os.Getenv("INPUT_QUEUE_PREFIX")
	if inputPrefix == "" {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("INPUT_QUEUE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filteraccountseen.FilterAccountSeenConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return filteraccountseen.FilterAccountSeenConfig{
		Id:                    id,
		ExpectedEOFs:          expectedEOFs,
		OutputMiddleware:      outputQueue,
		MomHost:               momHost,
		MomPort:               momPort,
		InputMiddlewarePrefix: inputPrefix,
		QueryID:               queryID,
	}, nil
}
