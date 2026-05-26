package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/join"
)

func loadConfig() (join.JoinConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return join.JoinConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	joinAmount, err := strconv.Atoi(os.Getenv("JOIN_AMOUNT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("JOIN_AMOUNT environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	leftEofs := os.Getenv("LEFT_EOFS_EXPECTED")
	if leftEofs == "" {
		return join.JoinConfig{}, errors.New("LEFT_EOFS_EXPECTED environment variable is required")
	}
	leftEofsNum, err := strconv.Atoi(leftEofs)
	if err != nil {
		return join.JoinConfig{}, errors.New("LEFT_EOFS_EXPECTED environment variable is required and must be a number")
	}

	if leftEofsNum < 1 {
		return join.JoinConfig{}, errors.New("LEFT_EOFS_EXPECTED must be at least 1")
	}

	rightEofs := os.Getenv("RIGHT_EOFS_EXPECTED")
	if rightEofs == "" {
		return join.JoinConfig{}, errors.New("RIGHT_EOFS_EXPECTED environment variable is required")
	}

	rightEofsNum, err := strconv.Atoi(rightEofs)
	if err != nil {
		return join.JoinConfig{}, errors.New("RIGHT_EOFS_EXPECTED environment variable is required and must be a number")
	}

	if rightEofsNum < 1 {
		return join.JoinConfig{}, errors.New("RIGHT_EOFS_EXPECTED must be at least 1")
	}
	leftInputQueue := os.Getenv("LEFT_INPUT_QUEUE")
	if leftInputQueue == "" {
		return join.JoinConfig{}, errors.New("LEFT_INPUT_QUEUE environment variable is required")
	}

	rightInputQueue := os.Getenv("RIGHT_INPUT_QUEUE")
	rightInputExchange := os.Getenv("RIGHT_INPUT_EXCHANGE")
	if rightInputQueue == "" && rightInputExchange == "" {
		return join.JoinConfig{}, errors.New("RIGHT_INPUT_QUEUE or RIGHT_INPUT_EXCHANGE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	return join.JoinConfig{
		Id:                 id,
		Amount:             joinAmount,
		MomHost:            momHost,
		MomPort:            momPort,
		LeftInputQueue:     leftInputQueue,
		RightInputQueue:    rightInputQueue,
		RightInputExchange: rightInputExchange,
		OutputQueue:        outputQueue,
		LeftEofsExpected:   leftEofsNum,
		RightEofsExpected:  rightEofsNum,
	}, nil
}
