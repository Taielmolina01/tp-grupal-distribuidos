package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/filter"
)

const (
	_DATE_LAYOUT            = "2006-01-02 15:04:05"
	_DATES_DIFFERENCE_GUARD = 10
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

	inputQueue := os.Getenv("INPUT_QUEUE")
	inputExchange := os.Getenv("INPUT_EXCHANGE")
	inputRoutingKeysStr := os.Getenv("INPUT_ROUTING_KEYS")
	inputRoutingKeys := []string{}
	if inputRoutingKeysStr != "" {
		inputRoutingKeys = splitter.Split(inputRoutingKeysStr, ",")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")

	outputQueue := os.Getenv("OUTPUT_QUEUE")

	outputRoutingKeysStr := os.Getenv("OUTPUT_ROUTING_KEYS")
	outputRoutingKeys := []string{}
	if outputRoutingKeysStr != "" {
		outputRoutingKeys = strings.Split(outputRoutingKeysStr, ",")
	}

	filterAmount := os.Getenv("FILTER_AMOUNT")
	if filterAmount == "" {
		return filter.FilterConfig{}, errors.New("FILTER_AMOUNT environment variable is required")
	}

	filterAmountInt, err := strconv.Atoi(filterAmount)
	if err != nil {
		return filter.FilterConfig{}, errors.New("FILTER_AMOUNT environment variable must be a number")
	}

	queryIdStr := os.Getenv("QUERY_ID")
	if queryIdStr == "" {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required")
	}
	queryId, err := strconv.Atoi(queryIdStr)
	if err != nil {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	rightInputExchange := os.Getenv("RIGHT_INPUT_EXCHANGE")
	rightInputQueue := os.Getenv("RIGHT_INPUT_QUEUE")
	leftInputQueue := os.Getenv("LEFT_INPUT_QUEUE")
	rightInputRoutingKeysStr := os.Getenv("RIGHT_INPUT_ROUTING_KEYS")
	rightInputRoutingKeys := []string{}
	if rightInputRoutingKeysStr != "" {
		rightInputRoutingKeys = splitter.Split(rightInputRoutingKeysStr, ",")
	}

	config := filter.FilterConfig{
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputQueue:            inputQueue,
		InputExchange:         inputExchange,
		InputRoutingKeys:      inputRoutingKeys,
		OutputExchange:        outputExchange,
		OutputQueue:           outputQueue,
		OutputRoutingKeys:     outputRoutingKeys,
		LeftInputQueue:        leftInputQueue,
		RightInputQueue:       rightInputQueue,
		FilterAmount:          filterAmountInt,
		QueryId:               uint8(queryId),
		RightInputExchange:    rightInputExchange,
		RightInputRoutingKeys: rightInputRoutingKeys,
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
		dateStr = strings.TrimSpace(dateStr)
		date, err := time.Parse(_DATE_LAYOUT, dateStr)
		if err != nil {
			return fmt.Errorf("DATE_RANGE environment variable has an invalid date:\nValue %s\nLayout: %s", dateStr, _DATE_LAYOUT)
		}
		dates = append(dates, date)
	}
	config.StartDateRange = dates[0]
	config.EndDateRange = dates[1]

	if config.StartDateRange.After(config.EndDateRange) {
		return errors.New("start date must be before end date in DATE_RANGE environment variable")
	}

	// Ensure the checked range uses End - Start and compare in days
	if config.EndDateRange.Sub(config.StartDateRange).Hours()/24 > float64(_DATES_DIFFERENCE_GUARD) {
		return fmt.Errorf("the range between start and end date in DATE_RANGE environment variable must be less than %d", _DATES_DIFFERENCE_GUARD)
	}

	return nil
}

func loadPaymentMethods(config *filter.FilterConfig) error {
	paymentFormatStr := os.Getenv("PAYMENT_FORMATS")
	if paymentFormatStr == "" {
		return errors.New("PAYMENT_FORMATS environment variable is required if FILTER_TYPE is DATE_RANGE_AND_PAYMENT")
	}
	paymentFormats := splitter.Split(paymentFormatStr, ",")
	if len(paymentFormats) < 1 {
		return errors.New("PAYMENT_FORMATS environment variable is required if FILTER_TYPE is DATE_RANGE_AND_PAYMENT")
	}
	config.PaymentFormats = paymentFormats
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

func loadBankDistinctVenv(config *filter.FilterConfig) error {
	outputQueues := os.Getenv("OUTPUT_QUEUES")
	if outputQueues == "" {
		return errors.New("OUTPUT_QUEUES environment variable is required if FILTER_TYPE is BANK_DISTINCT")
	}
	config.OutputQueues = strings.Split(outputQueues, ",")
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
	case filter.CONVERTED_AMOUNT_FILTER:
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
		if err := loadPaymentMethods(config); err != nil {
			return err
		}
	case filter.COUNT_AND_FILTER:
		if err := loadCountAndFilterVenv(config); err != nil {
			return err
		}
	case filter.DATE_RANGE_AND_SPLITTER:
		outputQueues := os.Getenv("OUTPUT_QUEUES")
		if outputQueues == "" {
			return errors.New("OUTPUT_QUEUES environment variable is required if FILTER_TYPE is DATE_RANGE_AND_SPLITTER")
		}
		config.OutputQueues = strings.Split(outputQueues, ",")
	case filter.TRANSFER_DISTINCT:
		break
	case filter.BANK_DISTINCT:
		if err := loadBankDistinctVenv(config); err != nil {
			return err
		}
	case filter.AVERAGE_FILTER:
		avgInputQueue := os.Getenv("AVG_INPUT_QUEUE")
		if avgInputQueue == "" {
			return errors.New("AVG_INPUT_QUEUE environment variable is required if FILTER_TYPE is AVERAGE_FILTER")
		}
		config.AvgInputQueue = avgInputQueue
		avgExpectedEofsStr := os.Getenv("AVG_EXPECTED_EOFS")
		if avgExpectedEofsStr == "" {
			return errors.New("AVG_EXPECTED_EOFS environment variable is required if FILTER_TYPE is AVERAGE_FILTER")
		}
		n, err := strconv.Atoi(avgExpectedEofsStr)
		if err != nil {
			return errors.New("AVG_EXPECTED_EOFS environment variable must be a number")
		}
		config.AvgExpectedEofs = n
	default:
		return errors.New("FILTER_TYPE environment variable hasn't a valid value")
	}

	return nil
}
