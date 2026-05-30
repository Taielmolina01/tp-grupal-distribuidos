package main

import (
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/prefilterq4"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := prefilterq4.NewPreFilterQ4(config)
	if err != nil {
		slog.Error("While initializing prefilterq4", "err", err)
		return 1
	}

	go server.HandleSignals()
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
