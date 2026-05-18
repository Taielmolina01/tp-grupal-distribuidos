package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/filter"
)

const (
	_DATE_LAYOUT = "2006-01-02 15:04:05"
)

func loadConfig() (filter.FilterConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
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

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return filter.FilterConfig{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return filter.FilterConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	config := filter.FilterConfig{
		Id:             id,
		MomHost:        momHost,
		MomPort:        momPort,
		InputExchange:  inputExchange,
		OutputExchange: outputExchange,
	}

	if err := loadFilterTypeConfig(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	return config, nil
}

// Helpers

func loadCurrenciesVenv(config *filter.FilterConfig) error {
	currencies := strings.Split(os.Getenv("CURRENCIES"), ",")
	if len(currencies) < 1 {
		return errors.New("CURRENCIES environment variable is required if FILTER_TYPE is CURRENCY")
	}
	config.Currencies = currencies
	return nil
}

func loadAmountVenv(config *filter.FilterConfig) error {
	amountStr := os.Getenv("AMOUNT")
	amount, err := strconv.ParseFloat(amountStr, 32)
	if err != nil {
		return errors.New("FILTER_AMOUNT environment variable is required if FILTER_TYPE is AMOUNT")
	}
	config.Amount = float32(amount)
	return nil
}

func loadDateRangeVenv(config *filter.FilterConfig) error {
	dateRangeStr := strings.Split(os.Getenv("DATE_RANGE"), ",")
	if len(dateRangeStr) != 2 {
		return errors.New("DATE_RANGE environment variable must have only two dates separated by a ','")
	}
	dates := []time.Time{}
	for _, dateStr := range dateRangeStr {
		date, err := time.Parse(dateStr, _DATE_LAYOUT)
		if err != nil {
			return fmt.Errorf("DATE_RANGE environment variable has an invalid date:\nValue %s\nLayout: %s", dateStr, _DATE_LAYOUT)
		}
		dates = append(dates, date)
	}
	config.StartDateRange = dates[0]
	config.EndDateRange = dates[1]
	return nil
}

func loadCountAndFilterVenv(config *filter.FilterConfig) error {
	amountTresholdStr := os.Getenv("AMOUNT_TRESHOLD")
	amountTreshold, err := strconv.Atoi(amountTresholdStr)
	if err != nil {
		return errors.New("AMOUNT_TRESHOLD environment variable is required if FILTER_TYPE is COUNT_AND_FILTER and must be a number")
	}
	config.AmountTreshold = amountTreshold
	return nil
}

func loadFilterTypeConfig(config *filter.FilterConfig) error {
	filterTypeVenv := os.Getenv("FILTER_TYPE")
	if filterTypeVenv == "" {
		return errors.New("FILTER_TYPE environment variable is required")
	}

	config.Type = filter.FilterType(filterTypeVenv)

	switch config.Type {
	case filter.CURRENCY:
		if err := loadCurrenciesVenv(config); err != nil {
			return err
		}
	case filter.AMOUNT:
		if err := loadAmountVenv(config); err != nil {
			return err
		}
	case filter.DATE_RANGE:
		if err := loadDateRangeVenv(config); err != nil {
			return err
		}
	case filter.DATE_RANGE_AND_PAYMENT:
		if err := loadDateRangeVenv(config); err != nil {
			return err
		}
	case filter.COUNT_AND_FILTER:
		if err := loadCountAndFilterVenv(config); err != nil {
			return err
		}
	case filter.DATE_RANGE_AND_SPLITTER:
		if err := loadDateRangeVenv(config); err != nil {
			return err
		}
	case filter.TRANSFER_DISTINCT:
		break
	case filter.ACCOUNT_DISTINCT:
		break
	case filter.AVERAGE_FILTER:
		if err := loadAmountVenv(config); err != nil {
			return err
		}
	default:
		return errors.New("FILTER_TYPE environment variable hasn't a valid value")
	}

	return nil
}
