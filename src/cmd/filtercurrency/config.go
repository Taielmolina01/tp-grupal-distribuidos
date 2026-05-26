package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/filter/filtercurrency"
)

func loadConfig() (filtercurrency.FilterCurrencyConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	currenciesRaw := os.Getenv("CURRENCIES")
	if currenciesRaw == "" {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("CURRENCIES environment variable is required")
	}
	currencies := strings.Split(currenciesRaw, ",")

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filtercurrency.FilterCurrencyConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return filtercurrency.FilterCurrencyConfig{
		Id:             id,
		Currencies:     currencies,
		MomHost:        momHost,
		MomPort:        momPort,
		InputExchange:  inputExchange,
		InputQueue:     inputQueue,
		OutputExchange: outputExchange,
		QueryID:        queryID,
	}, nil
}
