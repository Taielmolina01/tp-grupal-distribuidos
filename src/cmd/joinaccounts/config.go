package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	commonconfig "tp-grupal-distribuidos/internal/common/config"
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

	outputPrefix := os.Getenv("OUTPUT_MIDDLEWARE_PREFIX")
	if outputPrefix == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	inputPrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputPrefix == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	qualifiedExchange := os.Getenv("QUALIFIED_EXCHANGE")
	if qualifiedExchange == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("QUALIFIED_EXCHANGE environment variable is required")
	}

	peerAmount, err := strconv.Atoi(os.Getenv("PEER_AMOUNT"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("PEER_AMOUNT environment variable is required and must be a number")
	}

	threshold, err := strconv.Atoi(os.Getenv("THRESHOLD"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("THRESHOLD environment variable is required and must be a number")
	}

	maxBatchSize, err := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("MAX_BATCH_SIZE environment variable is required and must be a number")
	}

	maxBatchBytes, err := commonconfig.ParseMaxBatchBytes()
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, err
	}

	expectedEOFs, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return joinaccounts.JoinAccountsConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return joinaccounts.JoinAccountsConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required")
	}

	return joinaccounts.JoinAccountsConfig{
		Id:                     id,
		OutputMiddlewareAmount: outputAmount,
		OutputMiddlewarePrefix: outputPrefix,
		MomHost:                momHost,
		MomPort:                momPort,
		InputMiddlewarePrefix:  inputPrefix,
		QualifiedExchange:      qualifiedExchange,
		PeerAmount:             peerAmount,
		Threshold:              threshold,
		QueryID:                queryID,
		MaxBatchSize:           maxBatchSize,
		MaxBatchBytes:          maxBatchBytes,
		ExpectedEOFs:           expectedEOFs,
		PersistPath:            persistPath,
		PersistBatchSize:       persistBatchSize,
		PersistFlushInterval:   persistFlushInterval,
	}, nil
}
