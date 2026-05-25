package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/joinaccounts"
)

func loadConfig() (joinaccounts.JoinAccountsConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	outputAmount, err := strconv.Atoi(os.Getenv("OUTPUT_AMOUNT"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	outputPrefix := os.Getenv("OUTPUT_QUEUE_PREFIX")
	if outputPrefix == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("OUTPUT_QUEUE_PREFIX environment variable is required")
	}

	inputPrefix := os.Getenv("INPUT_QUEUE_PREFIX")
	if inputPrefix == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("INPUT_QUEUE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return joinaccounts.JoinAccountsConfig{
		Id:                     id,
		OutputMiddlewareAmount: outputAmount,
		OutputMiddlewarePrefix: outputPrefix,
		MomHost:                momHost,
		MomPort:                momPort,
		InputMiddlewarePrefix:  inputPrefix,
		QueryID:                queryID,
	}, nil
}
