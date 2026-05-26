package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/client"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFileAccounts := os.Getenv("INPUT_FILE_ACCOUNTS")
	if inputFileAccounts == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE_ACCOUNTS environment variable is required")
	}

	inputFileTrans := os.Getenv("INPUT_FILE_TRANS")
	if inputFileTrans == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE_TRANS environment variable is required")
	}

	outputFilePrefix := os.Getenv("OUTPUT_FILE_PREFIX")
	if outputFilePrefix == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE_PREFIX environment variable is required")
	}

	maxBatchSize := 100
	if v := os.Getenv("MAX_BATCH_SIZE"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return client.ClientConfig{}, errors.New("MAX_BATCH_SIZE must be an integer")
		}
		maxBatchSize = parsed
	}

	connectionAttempts := 3
	if v := os.Getenv("CONNECTION_ATTEMPTS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return client.ClientConfig{}, errors.New("CONNECTION_ATTEMPTS must be an integer")
		}
		if parsed <= 0 {
			return client.ClientConfig{}, errors.New("CONNECTION_ATTEMPTS must be greater than 0")
		}
		connectionAttempts = parsed
	}

	connectionAttemptDelayMs := 300
	if v := os.Getenv("CONNECTION_ATTEMPT_DELAY_MS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return client.ClientConfig{}, errors.New("CONNECTION_ATTEMPT_DELAY_MS must be an integer")
		}
		connectionAttemptDelayMs = parsed
	}

	return client.ClientConfig{
		ServerHost:               serverHost,
		ServerPort:               serverPort,
		InputFileAccounts:        inputFileAccounts,
		InputFileTrans:           inputFileTrans,
		OutputFilePrefix:         outputFilePrefix,
		MaxBatchSize:             maxBatchSize,
		ConnectionAttempts:       connectionAttempts,
		ConnectionAttemptDelayMs: connectionAttemptDelayMs,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
