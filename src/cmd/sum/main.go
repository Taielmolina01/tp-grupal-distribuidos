package main

import (
	"log/slog"
	"os"

	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/sum"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := sum.NewSumByPaymentFormat(config)
	if err != nil {
		slog.Error("While initializing sum", "err", err)
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
