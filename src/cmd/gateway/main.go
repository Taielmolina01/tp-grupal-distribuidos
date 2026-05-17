package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	accountsExchange := os.Getenv("ACCOUNTS_EXCHANGE")
	if accountsExchange == "" {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_EXCHANGE environment variable is required")
	}

	transfersExchange := os.Getenv("TRANSFERS_EXCHANGE")
	if transfersExchange == "" {
		return gateway.GatewayConfig{}, errors.New("TRANSFERS_EXCHANGE environment variable is required")
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

	return gateway.GatewayConfig{
		AccountsExchange:  accountsExchange,
		TransfersExchange: transfersExchange,
		ResultsQueue:      resultsQueue,
		ServerHost:        serverHost,
		ServerPort:        serverPort,
		MomHost:           momHost,
		MomPort:           momPort,
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
