package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/worker"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/filter"
)

func loadConfig() (filter.FilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return filter.FilterConfig{}, err
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return filter.FilterConfig{}, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return filter.FilterConfig{}, err
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filter.FilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	sumPrefix := os.Getenv("SUM_PREFIX")
	if sumPrefix == "" {
		return filter.FilterConfig{}, errors.New("SUM_PREFIX environment variable is required")
	}

	aggregationPrefix := os.Getenv("AGGREGATION_PREFIX")
	if aggregationPrefix == "" {
		return filter.FilterConfig{}, errors.New("AGGREGATION_PREFIX environment variable is required")
	}

	return filter.FilterConfig{
		Id:                id,
		MomHost:           momHost,
		MomPort:           momPort,
		InputQueue:        inputQueue,
		SumAmount:         sumAmount,
		SumPrefix:         sumPrefix,
		AggregationAmount: aggregationAmount,
		AggregationPrefix: aggregationPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	var server worker.Worker
	switch config.FilterType {
	case filter.CURRENCY:
		server, err = filter.CreateCurrencyFilter(config)
	case filter.AMOUNT:
		server, err = filter.CreateAmountFilter(config)
	case filter.DATE_RANGE:
		server, err = filter.CreateDateRangeFilter(config)
	case filter.DATE_RANGE_AND_PAYMENT:
		server, err = filter.CreateDateRangeAndPaymentMethod(config)

	default:
		slog.Error("While loading filter type", "err", errors.New("Invalid filter type"))
		return 1
	}

	if err != nil {
		slog.Error("While initializing filter", "err", err)
		return 1
	}

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
