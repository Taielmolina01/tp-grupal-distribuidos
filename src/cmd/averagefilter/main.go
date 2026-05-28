package main

import (
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/averagefilter"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := averagefilter.NewAverageFilter(config)
	if err != nil {
		slog.Error("While initializing average filter", "err", err)
		return 1
	}

	go server.HandleSignals()
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
