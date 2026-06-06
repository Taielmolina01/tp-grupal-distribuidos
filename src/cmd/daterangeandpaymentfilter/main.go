package main

import (
	"log/slog"
	"os"
	"tp-grupal-distribuidos/internal/filter/daterangeandpaymentfilter"
)

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := daterangeandpaymentfilter.CreateDateRangeAndPaymentMethod(config)

	go server.HandleSignals()

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
