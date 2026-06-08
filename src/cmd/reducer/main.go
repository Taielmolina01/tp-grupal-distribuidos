package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

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

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return reducer.ReducerConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return reducer.ReducerConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return reducer.ReducerConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	inputEofsExpected, _ := strconv.Atoi(os.Getenv("INPUT_EOFS_EXPECTED"))

	return reducer.ReducerConfig{
		MomHost:           momHost,
		MomPort:           momPort,
		QueryId:           uint8(queryID),
		InputQueue:        inputQueue,
		OutputQueues:      strings.Split(outputQueues, ","),
		InputEofsExpected: inputEofsExpected,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := reducer.CreateReducerCount(config)
	if err != nil {
		slog.Error("While initializing count reducer", "err", err)
		return 1
	}

	go server.HandleSignals()
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
