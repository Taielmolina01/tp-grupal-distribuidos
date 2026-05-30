package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/prefilterq4"
)

func loadConfig() (prefilterq4.PreFilterQ4Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return prefilterq4.PreFilterQ4Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return prefilterq4.PreFilterQ4Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return prefilterq4.PreFilterQ4Config{}, errors.New("MOM_HOST environment variable is required")
	}

	inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputPrefix == "" {
		return prefilterq4.PreFilterQ4Config{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return prefilterq4.PreFilterQ4Config{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	threshold, err := strconv.Atoi(os.Getenv("THRESHOLD"))
	if err != nil {
		return prefilterq4.PreFilterQ4Config{}, errors.New("THRESHOLD environment variable is required and must be a number")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return prefilterq4.PreFilterQ4Config{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return prefilterq4.PreFilterQ4Config{
		Id:                    id,
		InputMiddlewarePrefix: inputPrefix,
		OutputExchange:        outputExchange,
		Threshold:             threshold,
		MomHost:               momHost,
		MomPort:               momPort,
		QueryID:               uint8(queryID),
	}, nil
}
