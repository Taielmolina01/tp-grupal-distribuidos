package main

import (
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/aggregate"
	"tp-grupal-distribuidos/internal/common/pinger"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := aggregate.NewAvgAggregator(config)
	if err != nil {
		slog.Error("While initializing aggregate", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultPort)
	defer healthPinger.Close()

	go server.HandleSignals()
	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
