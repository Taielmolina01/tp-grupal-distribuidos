package main

import (
	"log/slog"
	"os"
	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/fetcher"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := fetcher.CreateFetcher(config)
	if err != nil {
		slog.Error("While creating fetcher", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultHealthPort)
	defer healthPinger.Close()

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
