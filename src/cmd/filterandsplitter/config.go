package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/filter/filterandsplitter"
)

const _DATE_LAYOUT = "2006-01-02 15:04:05"

func loadConfig() (filterandsplitter.FilterAndSplitterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	outputAmount, err := strconv.Atoi(os.Getenv("OUTPUT_AMOUNT"))
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("OUTPUT_AMOUNT environment variable is required and must be a number")
	}

	outputPrefix := os.Getenv("OUTPUT_MIDDLEWARE_PREFIX")
	if outputPrefix == "" {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("OUTPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	queryID, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	startDate, endDate, err := loadDateRange()
	if err != nil {
		return filterandsplitter.FilterAndSplitterConfig{}, err
	}

	inputShardKey := os.Getenv("INPUT_ROUTING_KEYS")

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return filterandsplitter.FilterAndSplitterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}

	return filterandsplitter.FilterAndSplitterConfig{
		Id:                     id,
		StartDate:              startDate,
		EndDate:                endDate,
		OutputMiddlewareAmount: outputAmount,
		OutputMiddlewarePrefix: outputPrefix,
		FilterCurrencyAmt: filterAmount,
		MomHost:                momHost,
		MomPort:                momPort,
		InputMiddlewareName:    os.Getenv("INPUT_EXCHANGE"),
		InputMiddlewareQueue:   os.Getenv("INPUT_QUEUE"),
		InputShardKey:          inputShardKey,
		QueryID:                uint8(queryID),
		PersistPath:            persistPath,
	}, nil
}

func loadDateRange() (time.Time, time.Time, error) {
	raw := os.Getenv("DATE_RANGE")
	parts := strings.Split(raw, ",")
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, errors.New("DATE_RANGE must contain exactly two dates separated by ','")
	}
	start, err := time.Parse(_DATE_LAYOUT, strings.TrimSpace(parts[0]))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parsing start date: %w", err)
	}
	end, err := time.Parse(_DATE_LAYOUT, strings.TrimSpace(parts[1]))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parsing end date: %w", err)
	}
	return start, end, nil
}
