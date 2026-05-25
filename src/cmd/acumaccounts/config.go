package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/acumaccounts"
)

func loadConfig() (acumaccounts.AcumAccountsConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return acumaccounts.AcumAccountsConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	outputAmount, err := strconv.Atoi(os.Getenv("OUTPUT_AMOUNT"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	outputPrefix := os.Getenv("OUTPUT_MIDDLEWARE_PREFIX")
	if outputPrefix == "" {
		return acumaccounts.AcumAccountsConfig{}, errors.New("OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputPrefix == "" {
		return acumaccounts.AcumAccountsConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	requiredAmt, err := strconv.Atoi(os.Getenv("REQUIRED_AMT"))
	if err != nil {
		return acumaccounts.AcumAccountsConfig{}, errors.New("REQUIRED_AMT environment variable is required and must be a number")
	}

	return acumaccounts.AcumAccountsConfig{
		Id:                     id,
		OutputMiddlewareAmount: outputAmount,
		OutputMiddlewarePrefix: outputPrefix,
		MomHost:                momHost,
		MomPort:                momPort,
		ExpectedEOFs:           expectedEOFs,
		InputMiddlewarePrefix:  inputPrefix,
		QueryID:                queryID,
		RequiredAmt:            requiredAmt,
	}, nil
}
