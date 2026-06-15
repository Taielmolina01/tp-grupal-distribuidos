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

	aggregateAmount, err := strconv.Atoi(os.Getenv("AGGREGATE_AMOUNT"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("AGGREGATE_AMOUNT environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return aggregate.AggregateConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return aggregate.AggregateConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return aggregate.AggregateConfig{}, errors.New("SUM_AMOUNT environment variable is required and must be a number")
	}

	return aggregate.AggregateConfig{
		Id:              id,
		AggregateAmount: aggregateAmount,
		SumAmount:       sumAmount,
		MomHost:         momHost,
		MomPort:         momPort,
		QueryID:         uint8(queryID),
		InputQueue:      inputQueue,
		OutputQueues:    strings.Split(outputQueues, ","),
	}, nil
}
