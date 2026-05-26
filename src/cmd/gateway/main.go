package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/gateway"
)

const QUEUES_SEPARATOR = ","

func loadConfig() (gateway.GatewayConfig, error) {
	accountsExchange := os.Getenv("ACCOUNTS_EXCHANGE")
	if accountsExchange == "" {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_EXCHANGE environment variable is required")
	}

	accountsShardsStr := os.Getenv("ACCOUNTS_SHARDS")
	if accountsShardsStr == "" {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_SHARDS environment variable is required")
	}
	accountsShards, err := strconv.Atoi(accountsShardsStr)
	if err != nil || accountsShards <= 0 {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_SHARDS must be a positive integer")
	}

	transfersExchange := os.Getenv("TRANSFERS_EXCHANGE")
	if transfersExchange == "" {
		return gateway.GatewayConfig{}, errors.New("TRANSFERS_EXCHANGE environment variable is required")
	}

	transfersRoutingKeysStr := os.Getenv("TRANSFERS_ROUTING_KEYS")
	transfersRoutingKeys := []string{}
	if transfersRoutingKeysStr != "" {
		transfersRoutingKeys = splitter.Split(transfersRoutingKeysStr, QUEUES_SEPARATOR)
	}

	resultsQueue := os.Getenv("RESULTS_QUEUE")
	if resultsQueue == "" {
		return gateway.GatewayConfig{}, errors.New("RESULTS_QUEUE environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return gateway.GatewayConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	maxBatchSize := 100
	if v := os.Getenv("MAX_BATCH_SIZE"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return gateway.GatewayConfig{}, errors.New("MAX_BATCH_SIZE must be an integer")
		}
		maxBatchSize = parsed
	}

	queryEOFsStr := os.Getenv("QUERY_EOFS_EXPECTED")
	if queryEOFsStr == "" {
		return gateway.GatewayConfig{}, errors.New("QUERY_EOFS_EXPECTED environment variable is required")
	}
	queryEOFsExpected := map[uint8]int{}
	for pair := range strings.SplitSeq(queryEOFsStr, QUEUES_SEPARATOR) {
		pair = strings.TrimSpace(pair)
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid QUERY_EOFS_EXPECTED entry: %q (expected format queryID:count)", pair)
		}
		qid, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 8)
		if err != nil {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid query id in QUERY_EOFS_EXPECTED: %q", parts[0])
		}
		count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || count <= 0 {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid EOF count in QUERY_EOFS_EXPECTED (must be > 0): %q", parts[1])
		}
		queryEOFsExpected[uint8(qid)] = count
	}
	if len(queryEOFsExpected) == 0 {
		return gateway.GatewayConfig{}, errors.New("QUERY_EOFS_EXPECTED must define at least one query")
	}

	return gateway.GatewayConfig{
		AccountsExchange:     accountsExchange,
		AccountsShards:       accountsShards,
		TransfersExchange:    transfersExchange,
		TransfersRoutingKeys: transfersRoutingKeys,
		ResultsQueue:         resultsQueue,
		ServerHost:           serverHost,
		ServerPort:           serverPort,
		MomHost:              momHost,
		MomPort:              momPort,
		MaxBatchSize:         maxBatchSize,
		QueryEOFsExpected:    queryEOFsExpected,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While initializing gateway", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
