package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/reducer"
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

	return reducer.ReducerConfig{
		MomHost:        momHost,
		MomPort:        momPort,
		InputExchange:  inputExchange,
		OutputExchange: outputExchange,
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
