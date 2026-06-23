package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/common/filter"
)

func loadConfig() (filter.FilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("ID environment variable is required and must be a number")
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
	if inputQueue == "" {
		return filter.FilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return filter.FilterConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	queryId, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return filter.FilterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}

	return filter.FilterConfig{
		Id:                   id,
		MomHost:              momHost,
		MomPort:              momPort,
		InputQueue:           inputQueue,
		OutputQueues:         strings.Split(outputQueues, ","),
		QueryID:              uint8(queryId),
		PersistPath:          persistPath,
		PersistBatchSize:     persistBatchSize,
		PersistFlushInterval: persistFlushInterval,
	}, nil
}
