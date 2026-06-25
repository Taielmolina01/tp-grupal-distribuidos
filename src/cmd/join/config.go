package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"tp-grupal-distribuidos/internal/join"
)

func loadConfig() (join.JoinConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return join.JoinConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	leftEofsNum, err := strconv.Atoi(os.Getenv("LEFT_EOFS_EXPECTED"))
	if err != nil || leftEofsNum < 1 {
		return join.JoinConfig{}, errors.New("LEFT_EOFS_EXPECTED environment variable is required and must be a number >= 1")
	}

	rightEofsNum, err := strconv.Atoi(os.Getenv("RIGHT_EOFS_EXPECTED"))
	if err != nil || rightEofsNum < 1 {
		return join.JoinConfig{}, errors.New("RIGHT_EOFS_EXPECTED environment variable is required and must be a number >= 1")
	}

	leftInputQueue := os.Getenv("LEFT_INPUT_MIDDLEWARE_PREFIX")
	if leftInputQueue == "" {
		return join.JoinConfig{}, errors.New("LEFT_INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	rightInputQueue := os.Getenv("RIGHT_INPUT_MIDDLEWARE_PREFIX")
	if rightInputQueue == "" {
		return join.JoinConfig{}, errors.New("RIGHT_INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return join.JoinConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return join.JoinConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}

	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return join.JoinConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}

	return join.JoinConfig{
		Id:                         id,
		MomHost:                    momHost,
		MomPort:                    momPort,
		LeftInputMiddlewarePrefix:  leftInputQueue,
		RightInputMiddlewarePrefix: rightInputQueue,
		OutputQueue:                outputQueue,
		LeftEofsExpected:     leftEofsNum,
		RightEofsExpected:    rightEofsNum,
		PersistPath:          persistPath,
		PersistBatchSize:     persistBatchSize,
		PersistFlushInterval: persistFlushInterval,
	}, nil
}
