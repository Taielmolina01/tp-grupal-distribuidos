package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/bankdistinct"
)

func loadConfig() (bankdistinct.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return bankdistinct.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return bankdistinct.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return bankdistinct.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return bankdistinct.Config{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return bankdistinct.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return bankdistinct.Config{}, errors.New("OUTPUT_QUEUES environment variable is required")
	}

	return bankdistinct.Config{
		Id:           id,
		MomHost:      momHost,
		MomPort:      momPort,
		InputQueue:   inputQueue,
		OutputQueues: strings.Split(outputQueues, ","),
		QueryID:      uint8(queryID),
	}, nil
}
