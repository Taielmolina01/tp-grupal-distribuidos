package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/join"
)

func loadConfig() (join.JoinConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	leftInputExchange := os.Getenv("LEFT_INPUT_EXCHANGE")
	if leftInputExchange == "" {
		return join.JoinConfig{}, errors.New("LEFT_INPUT_EXCHANGE environment variable is required")
	}

	rightInputExchange := os.Getenv("RIGHT_INPUT_EXCHANGE")
	if rightInputExchange == "" {
		return join.JoinConfig{}, errors.New("RIGHT_INPUT_EXCHANGE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	return join.JoinConfig{
		MomHost:            momHost,
		MomPort:            momPort,
		LeftInputExchange:  leftInputExchange,
		RightInputExchange: rightInputExchange,
		OutputExchange:     outputExchange,
	}, nil
}
