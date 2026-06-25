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

	if err := loadCurrenciesVenv(&config); err != nil {
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

func loadCurrenciesVenv(config *filter.FilterConfig) error {
	currencies := strings.Split(os.Getenv("CURRENCIES"), ",")
	if len(currencies) < 1 {
		return errors.New("CURRENCIES environment variable is required")
	}
	config.Currencies = currencies
	return nil
}
