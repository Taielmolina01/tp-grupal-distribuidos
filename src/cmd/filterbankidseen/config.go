package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/filterbankidseen"
)

func loadConfig() (filterbankidseen.FilterBankIdSeenConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	outputAmount, err := strconv.Atoi(os.Getenv("OUTPUT_AMOUNT"))
	if err != nil || outputAmount <= 0 {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required and must be a positive number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filterbankidseen.FilterBankIdSeenConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return filterbankidseen.FilterBankIdSeenConfig{
		Id:             id,
		InputExchange:  inputExchange,
		OutputExchange: outputExchange,
		OutputAmount:   outputAmount,
		MomHost:        momHost,
		MomPort:        momPort,
		QueryID:        queryID,
	}, nil
}
