package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/averagefilter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/inner"
)

func loadConfig() (averagefilter.AverageFilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputTransfersQueue := os.Getenv("INPUT_QUEUE")
	if inputTransfersQueue == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	inputAvgsQueue := os.Getenv("AVG_INPUT_QUEUE")
	if inputAvgsQueue == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("AVG_INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return averagefilter.AverageFilterConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	avgsExpectedEofs, err := strconv.Atoi(os.Getenv("AVG_EXPECTED_EOFS"))
	if err != nil {
		return averagefilter.AverageFilterConfig{}, errors.New("AVG_EXPECTED_EOFS environment variable is required and must be a number")
	}

	return averagefilter.AverageFilterConfig{
		Id:                  id,
		FilterAmount:        filterAmount,
		MomHost:             momHost,
		MomPort:             momPort,
		InputTransfersQueue: inputTransfersQueue,
		InputAvgsQueue:      inputAvgsQueue,
		OutputQueue:         outputQueue,
		AvgsExpectedEofs:    avgsExpectedEofs,
		QueryID:             inner.Query3ID,
	}, nil
}
