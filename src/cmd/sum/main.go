package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/sum"
)

func loadConfig() (sum.SumConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return sum.SumConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return sum.SumConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("SUM_AMOUNT environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return sum.SumConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return sum.SumConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return sum.SumConfig{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	inputEofsExpected, _ := strconv.Atoi(os.Getenv("INPUT_EOFS_EXPECTED"))

	return sum.SumConfig{
		Id:                id,
		SumAmount:         sumAmount,
		MomHost:           momHost,
		MomPort:           momPort,
		QueryID:           uint8(queryID),
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

	server, err := sum.CreateSumByPaymentFormat(config)
	if err != nil {
		slog.Error("While initializing sum", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
