package main

import (
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/filter/filtercurrency"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := filtercurrency.NewFilterCurrency(config)
	if err != nil {
		slog.Error("While initializing filter-currency", "err", err)
		return 1
	}

	go server.HandleSignals()
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
