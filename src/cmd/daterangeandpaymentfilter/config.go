package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	commonconfig "tp-grupal-distribuidos/internal/common/config"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/splitter"
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

	inputMiddlewarePrefix := os.Getenv("INPUT_MIDDLEWARE_PREFIX")
	if inputMiddlewarePrefix == "" {
		return filter.FilterConfig{}, errors.New("INPUT_MIDDLEWARE_PREFIX environment variable is required")
	}

	filterAmountInt, err := strconv.Atoi(os.Getenv("EXPECTED_EOFS"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("EXPECTED_EOFS environment variable is required and must be a number")
	}

	queryId, err := strconv.Atoi(os.Getenv("QUERY_ID"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("QUERY_ID environment variable is required and must be a number")
	}

	outputClusters, err := loadOutputClusters()
	if err != nil {
		return filter.FilterConfig{}, err
	}

	config := filter.FilterConfig{
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		InputMiddlewarePrefix: inputMiddlewarePrefix,
		FilterAmount:          filterAmountInt,
		QueryID:               uint8(queryId),
		OutputClusters:        outputClusters,
	}

	if err := loadDateRangeVenv(&config); err != nil {
		return filter.FilterConfig{}, err
	}
	if err := loadPaymentMethods(&config); err != nil {
		return filter.FilterConfig{}, err
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return filter.FilterConfig{}, errors.New("PERSIST_PATH environment variable is required")
	}
	persistBatchSize, err := strconv.Atoi(os.Getenv("PERSIST_BATCH_SIZE"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_BATCH_SIZE environment variable is required and must be a number")
	}
	persistFlushInterval, err := time.ParseDuration(os.Getenv("PERSIST_FLUSH_INTERVAL"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("PERSIST_FLUSH_INTERVAL environment variable is required (e.g. '1s')")
	}
	config.PersistPath = persistPath
	config.PersistBatchSize = persistBatchSize
	config.PersistFlushInterval = persistFlushInterval

	senderTrackerCapacity, err := commonconfig.ParseSenderTrackerCapacity()
	if err != nil {
		return filter.FilterConfig{}, err
	}
	config.SenderTrackerCapacity = senderTrackerCapacity

	return config, nil
}

func loadOutputClusters() ([]shard.ClusterConfig, error) {
	clustersStr := os.Getenv("OUTPUT_CLUSTERS")
	if clustersStr == "" {
		return nil, errors.New("OUTPUT_CLUSTERS environment variable is required")
	}
	parts := splitter.Split(clustersStr, ",")
	clusters := make([]shard.ClusterConfig, 0, len(parts))
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid OUTPUT_CLUSTERS entry %q (expected prefix:N)", part)
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid node count in OUTPUT_CLUSTERS entry %q: %w", part, err)
		}
		clusters = append(clusters, shard.ClusterConfig{
			Prefix:    strings.TrimSpace(kv[0]),
			NodeCount: n,
		})
	}
	return clusters, nil
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

	if config.EndDateRange.Sub(config.StartDateRange).Hours()/24 > float64(_DATES_DIFFERENCE_GUARD) {
		return fmt.Errorf("the range between start and end date in DATE_RANGE environment variable must be less than %d", _DATES_DIFFERENCE_GUARD)
	}

	return nil
}

func loadPaymentMethods(config *filter.FilterConfig) error {
	paymentFormatStr := os.Getenv("PAYMENT_FORMATS")
	if paymentFormatStr == "" {
		return errors.New("PAYMENT_FORMATS environment variable is required")
	}
	paymentFormats := splitter.Split(paymentFormatStr, ",")
	if len(paymentFormats) < 1 {
		return errors.New("PAYMENT_FORMATS environment variable is required")
	}
	config.PaymentFormats = paymentFormats
	return nil
}
