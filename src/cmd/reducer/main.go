package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/reducer"
)

func loadConfig() (reducer.ReducerConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return reducer.ReducerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	queryId, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return reducer.ReducerConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}
	outputQueuesStr := strings.Split(outputQueues, ",")

	reducerTypeStr := os.Getenv("REDUCER_TYPE")
	if reducerTypeStr == "" {
		return reducer.ReducerConfig{}, errors.New("REDUCER_TYPE environment variable is required")
	}
	reducerType := reducer.ReducerType(reducerTypeStr)

	config := reducer.ReducerConfig{
		Id:           id,
		MomHost:      momHost,
		MomPort:      momPort,
		QueryID:      uint8(queryId),
		OutputQueues: outputQueuesStr,
		ReducerType:  reducerType,
	}

	switch reducerType {
	case reducer.MAX_AMOUNT_FROM_BANK:
		inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
		if inputPrefix == "" {
			return reducer.ReducerConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
		}

		expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
		if err != nil {
			return reducer.ReducerConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
		}

		persistPath := os.Getenv("PERSIST_PATH")
		if persistPath == "" {
			return reducer.ReducerConfig{}, errors.New("PERSIST_PATH environment variable is required")
		}

		persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
		if err != nil {
			return reducer.ReducerConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
		}

		persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
		if err != nil {
			return reducer.ReducerConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
		}

		config.InputMiddlewarePrefix = inputPrefix
		config.ExpectedEOFs = expectedEOFs
		config.PersistPath = persistPath
		config.PersistBatchSize = persistBatchSize
		config.PersistFlushInterval = persistFlushInterval
	case reducer.COUNT:
		inputQueue := os.Getenv("INPUT_QUEUE")
		if inputQueue == "" {
			return reducer.ReducerConfig{}, errors.New("INPUT_QUEUE environment variable is required")
		}
		inputEofsExpected, _ := strconv.Atoi(os.Getenv("INPUT_EOFS_EXPECTED"))

		persistPath := os.Getenv("PERSIST_PATH")
		if persistPath == "" {
			return reducer.ReducerConfig{}, errors.New("PERSIST_PATH environment variable is required")
		}
		persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
		if err != nil {
			return reducer.ReducerConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
		}
		persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
		if err != nil {
			return reducer.ReducerConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
		}

		config.InputQueue = inputQueue
		config.InputEofsExpected = inputEofsExpected
		config.PersistPath = persistPath
		config.PersistBatchSize = persistBatchSize
		config.PersistFlushInterval = persistFlushInterval
	default:
		return reducer.ReducerConfig{}, fmt.Errorf("invalid REDUCER_TYPE: %s", reducerType)
	}

	return config, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	var server worker.Worker
	switch config.ReducerType {
	case reducer.MAX_AMOUNT_FROM_BANK:
		server, err = reducer.CreateReducerMaxAmountFromBank(config)
	case reducer.COUNT:
		server, err = reducer.CreateReducerCount(config)
	default:
		slog.Error("Invalid reducer type", "type", config.ReducerType)
		return 1
	}
	if err != nil {
		slog.Error("While initializing join", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultHealthPort)
	defer healthPinger.Close()

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
