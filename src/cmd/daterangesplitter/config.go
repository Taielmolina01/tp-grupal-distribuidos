package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
	"tp-grupal-distribuidos/internal/common/splitter"
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

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	inputQueue := os.Getenv("INPUT_QUEUE")

	inputRoutingKeys := []string{}
	if v := os.Getenv("INPUT_ROUTING_KEYS"); v != "" {
		inputRoutingKeys = splitter.Split(v, ",")
	}

	outputQueuesStr := os.Getenv("OUTPUT_QUEUES")
	if outputQueuesStr == "" {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}
	outputQueues := strings.Split(outputQueuesStr, ",")

	splitterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return daterangesplitter.DateRangeSplitterConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	return daterangesplitter.DateRangeSplitterConfig{
		Id:                id,
		SplitterAmount:    splitterAmount,
		MomHost:           momHost,
		MomPort:           momPort,
		InputExchange:     inputExchange,
		InputQueue:        inputQueue,
		InputRoutingKeys:  inputRoutingKeys,
		OutputQueues:      outputQueues,
		QueryID:           inner.Query3ID,
		AvgPeriodStart:    time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC),
		AvgPeriodEnd:      time.Date(2022, 9, 5, 23, 59, 59, 0, time.UTC),
		FilterPeriodStart: time.Date(2022, 9, 6, 0, 0, 0, 0, time.UTC),
		FilterPeriodEnd:   time.Date(2022, 9, 15, 23, 59, 59, 0, time.UTC),
	}, nil
}
