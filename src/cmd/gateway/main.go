package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/gateway"
)

const QUEUES_SEPARATOR = ","

func loadConfig() (gateway.GatewayConfig, error) {
	accountQueues := os.Getenv("ACCOUNT_QUEUES")
	if accountQueues == "" {
		return gateway.GatewayConfig{}, errors.New("ACCOUNT_QUEUES environment variable is required")
	}

	accountQueueList := splitter.Split(accountQueues, QUEUES_SEPARATOR)

	transfersQueues := os.Getenv("TRANSFERS_QUEUES")
	if transfersQueues == "" {
		return gateway.GatewayConfig{}, errors.New("TRANSFERS_QUEUES environment variable is required")
	}
	transfersQueueList := splitter.Split(transfersQueues, QUEUES_SEPARATOR)

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

	return gateway.GatewayConfig{
		AccountQueues:        accountQueueList,
		TransfersQueues:      transfersQueueList,
		TransfersExchange:    transfersExchange,
		TransfersRoutingKeys: transfersRoutingKeys,
		ResultsQueue:         resultsQueue,
		ServerHost:           serverHost,
		ServerPort:           serverPort,
		MomHost:              momHost,
		MomPort:              momPort,
		MaxBatchSize:         maxBatchSize,
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
