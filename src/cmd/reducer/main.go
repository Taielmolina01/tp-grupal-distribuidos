package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

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

	inputExchange := os.Getenv("INPUT_EXCHANGE")

	queryId, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	reducerAmount, err := strconv.Atoi(os.Getenv("REDUCER_AMOUNT"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("REDUCER_AMOUNT environment variable is required and must be a number")
	}

	inputRoutingKeysStr := os.Getenv("INPUT_ROUTING_KEYS")
	inputRoutingKeys := []string{}
	if inputRoutingKeysStr != "" {
		inputRoutingKeys = strings.Split(inputRoutingKeysStr, ",")
	}

	inputEofsExpected, _ := strconv.Atoi(os.Getenv("INPUT_EOFS_EXPECTED"))

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return reducer.ReducerConfig{}, errors.New("INPUT_QUEUE environment variable is required")
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

	return reducer.ReducerConfig{
		Id:                id,
		ReducerAmount:     reducerAmount,
		MomHost:           momHost,
		MomPort:           momPort,
		QueryId:           uint8(queryId),
		InputExchange:     inputExchange,
		InputQueue:        inputQueue,
		InputRoutingKeys:  inputRoutingKeys,
		OutputQueues:      outputQueuesStr,
		InputEofsExpected: inputEofsExpected,
		ReducerType:       reducerType,
	}, nil
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

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
