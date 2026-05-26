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

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return joinaccounts.JoinAccountsConfig{
		Id:             id,
		OutputAmount:   outputAmount,
		OutputExchange: outputExchange,
		MomHost:        momHost,
		MomPort:        momPort,
		InputExchange:  inputExchange,
		QueryID:        queryID,
	}, nil
}
