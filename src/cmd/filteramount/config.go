package main

import (
	"errors"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/filter/filteramount"
)

func loadConfig() (filteramount.FilterAmountConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filteramount.FilterAmountConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filteramount.FilterAmountConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filteramount.FilterAmountConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	amountRaw, err := strconv.ParseFloat(os.Getenv("AMOUNT"), 32)
	if err != nil {
		return filteramount.FilterAmountConfig{}, errors.New("AMOUNT environment variable is required and must be a number")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return filteramount.FilterAmountConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return filteramount.FilterAmountConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filteramount.FilterAmountConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filteramount.FilterAmountConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	return filteramount.FilterAmountConfig{
		Id:            id,
		Amount:        float32(amountRaw),
		OutputQueue:   outputQueue,
		MomHost:       momHost,
		MomPort:       momPort,
		InputExchange: inputExchange,
		InputQueue:    inputQueue,
		QueryID:       queryID,
	}, nil
}
