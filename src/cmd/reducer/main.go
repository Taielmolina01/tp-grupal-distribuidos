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

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return reducer.ReducerConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return reducer.ReducerConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

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

	outputRoutingKeysStr := os.Getenv("OUTPUT_ROUTING_KEYS")
	outputRoutingKeys := []string{}
	if outputRoutingKeysStr != "" {
		outputRoutingKeys = strings.Split(outputRoutingKeysStr, ",")
	}

	inputEofsExpected, _ := strconv.Atoi(os.Getenv("INPUT_EOFS_EXPECTED"))

	return reducer.ReducerConfig{
		Id:                id,
		ReducerAmount:     reducerAmount,
		MomHost:           momHost,
		MomPort:           momPort,
		InputExchange:     inputExchange,
		OutputExchange:    outputExchange,
		QueryId:           uint8(queryId),
		InputQueue:        os.Getenv("INPUT_QUEUE"),
		OutputQueue:       os.Getenv("OUTPUT_QUEUE"),
		InputRoutingKeys:  inputRoutingKeys,
		OutputRoutingKeys: outputRoutingKeys,
		InputEofsExpected: inputEofsExpected,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := reducer.CreateReducerMaxAmountFromBank(config)
	if err != nil {
		slog.Error("While initializing join", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
