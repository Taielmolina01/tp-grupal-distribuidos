package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/bankjoin"
)

func loadConfig() (bankjoin.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return bankjoin.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return bankjoin.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return bankjoin.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	leftEofsNum, err := strconv.Atoi(os.Getenv("LEFT_EOFS_EXPECTED"))
	if err != nil {
		return bankjoin.Config{}, errors.New("LEFT_EOFS_EXPECTED environment variable is required and must be a number")
	}
	if leftEofsNum < 1 {
		return bankjoin.Config{}, errors.New("LEFT_EOFS_EXPECTED must be at least 1")
	}

	rightEofsNum, err := strconv.Atoi(os.Getenv("RIGHT_EOFS_EXPECTED"))
	if err != nil {
		return bankjoin.Config{}, errors.New("RIGHT_EOFS_EXPECTED environment variable is required and must be a number")
	}
	if rightEofsNum < 1 {
		return bankjoin.Config{}, errors.New("RIGHT_EOFS_EXPECTED must be at least 1")
	}

	leftInputQueue := os.Getenv("LEFT_INPUT_QUEUE")
	if leftInputQueue == "" {
		return bankjoin.Config{}, errors.New("LEFT_INPUT_QUEUE environment variable is required")
	}

	rightInputQueue := os.Getenv("RIGHT_INPUT_QUEUE")
	if rightInputQueue == "" {
		return bankjoin.Config{}, errors.New("RIGHT_INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return bankjoin.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	return bankjoin.Config{
		Id:                id,
		MomHost:           momHost,
		MomPort:           momPort,
		LeftInputQueue:    leftInputQueue,
		RightInputQueue:   rightInputQueue,
		OutputQueue:       outputQueue,
		LeftEofsExpected:  leftEofsNum,
		RightEofsExpected: rightEofsNum,
	}, nil
}
