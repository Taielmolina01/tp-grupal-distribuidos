package main

import (
	"errors"
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	var server worker.Worker
	switch config.Type {
	case filter.CURRENCY:
		server, err = filter.CreateCurrencyFilter(config)
	case filter.AMOUNT:
		server, err = filter.CreateAmountFilter(config)
	case filter.DATE_RANGE:
		server, err = filter.CreateDateRangeFilter(config)
	case filter.DATE_RANGE_AND_PAYMENT:
		server, err = filter.CreateDateRangeAndPaymentMethod(config)
	case filter.BANK_DISTINCT:
		server, err = filter.CreateBankDistinctFilter(config)
	case filter.DATE_RANGE_AND_SPLITTER:
		server, err = filter.CreateDateAndRangeSplitter(config)
	case filter.AVERAGE_FILTER:
		server, err = filter.CreateAverageFilter(config)
	default:
		slog.Error("While loading filter type", "err", errors.New("invalid filter type"))
		return 1
	}

	if err != nil {
		slog.Error("While initializing filter", "err", err)
		return 1
	}

	go server.HandleSignals()

	server.Run()
	slog.Info("FUME FUME FUME FUMETEO")
	return 0
}

func main() {
	os.Exit(run())
}
