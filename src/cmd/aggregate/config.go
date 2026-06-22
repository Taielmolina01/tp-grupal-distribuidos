package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/aggregate"
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

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return aggregate.AggregateConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	return aggregate.AggregateConfig{
		Id:                    id,
		ExpectedEOFs:          expectedEOFs,
		MomHost:               momHost,
		MomPort:               momPort,
		QueryID:               uint8(queryID),
		InputMiddlewarePrefix: inputPrefix,
		OutputQueues:          strings.Split(outputQueues, ","),
	}, nil
}
