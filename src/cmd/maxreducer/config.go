package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/maxreducer"
)

func loadConfig() (maxreducer.Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return maxreducer.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return maxreducer.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return maxreducer.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	reducerAmount, err := strconv.Atoi(os.Getenv("REDUCER_AMOUNT"))
	if err != nil {
		return maxreducer.Config{}, errors.New("REDUCER_AMOUNT environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return maxreducer.Config{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return maxreducer.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return maxreducer.Config{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	inputRoutingKeys := []string{}
	if v := os.Getenv("INPUT_ROUTING_KEYS"); v != "" {
		inputRoutingKeys = strings.Split(v, ",")
	}

	return maxreducer.Config{
		Id:               id,
		ReducerAmount:    reducerAmount,
		MomHost:          momHost,
		MomPort:          momPort,
		InputExchange:    os.Getenv("INPUT_EXCHANGE"),
		InputQueue:       inputQueue,
		InputRoutingKeys: inputRoutingKeys,
		OutputQueues:     strings.Split(outputQueues, ","),
		QueryID:          uint8(queryID),
	}, nil
}
