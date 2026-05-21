package main

import (
	"errors"
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/join"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	joinTypeStr := os.Getenv("JOIN_TYPE")
	if joinTypeStr == "" {
		slog.Error("While loading join type", "err", errors.New("JOIN_TYPE environment variable is required"))
		return 1
	}

	var server worker.Worker
	switch join.JoinType(joinTypeStr) {
	case join.TransferAccountByBank:
		server, err = join.CreateTransferAccountByBankJoin(config)
	case join.SplittedTransfer:
		server, err = join.CreateSplittedTransferJoin(config)
	default:
		slog.Error("While loading join type", "err", errors.New("invalid join type"))
		return 1
	}

	if err != nil {
		slog.Error("While initializing join", "err", err)
		return 1
	}

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
