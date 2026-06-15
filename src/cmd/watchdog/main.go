package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-grupal-distribuidos/internal/common/bully"
	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/watchdog/healthcheck"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultHealthPort)
	defer healthPinger.Close()

	baseBullyPort, err := strconv.Atoi(config.BullyPort)
	if err != nil {
		slog.Error("Invalid BULLY_PORT", "port", config.BullyPort, "err", err)
		return 1
	}

	bullyAlg, err := bully.CreateBullyAlgorithm(int(config.Id), int(config.Amount), baseBullyPort)
	if err != nil {
		slog.Error("While creating bully algorithm", "err", err)
		return 1
	}

	checker := healthcheck.New(config, bullyAlg)

	bullyAlg.SetLeaderChangeCallback(func(leaderId int) {
		checker.UpdateBullyInfo(healthcheck.BullyInfo{LeaderId: uint8(leaderId)})
	})

	go func() {
		if err := bullyAlg.Run(); err != nil {
			slog.Error("Bully algorithm error", "err", err)
		}
	}()

	checker.Start()

	slog.Info(fmt.Sprintf("Watchdog %d started", config.Id))

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
