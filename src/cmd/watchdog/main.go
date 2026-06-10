package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/watchdog/healthcheck"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultPort)
	defer healthPinger.Close()

	checker := healthcheck.New(config)
	checker.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping watchdog")

	checker.Stop()
	return 0
}

func main() {
	os.Exit(run())
}
