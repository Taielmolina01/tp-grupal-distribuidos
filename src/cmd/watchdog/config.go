package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/watchdog/healthcheck"
)

func loadConfig() (healthcheck.Config, error) {
	nodes := strings.Fields(os.Getenv("NODES"))
	if len(nodes) == 0 {
		return healthcheck.Config{}, errors.New("NODES environment variable is required (space-separated container names)")
	}

	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = pinger.DefaultHealthPort
	}

	bullyPort := os.Getenv("BULLY_PORT")
	if bullyPort == "" {
		bullyPort = pinger.DefaultBullyPort
	}

	interval := durationEnv("INTERVAL", 500*time.Millisecond)
	timeout := durationEnv("TIMEOUT", 400*time.Millisecond)
	startup := durationEnv("STARTUP", 5*time.Second)

	maxRetries := 3
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return healthcheck.Config{}, errors.New("MAX_RETRIES must be a number")
		}
		maxRetries = n
	}

	amount := os.Getenv("AMOUNT")
	if amount == "" {
		return healthcheck.Config{}, errors.New("AMOUNT environment variable is required (number of nodes to monitor)")
	}
	n, err := strconv.Atoi(amount)
	if err != nil {
		return healthcheck.Config{}, errors.New("AMOUNT must be a number")
	}

	idStr := os.Getenv("ID")
	if idStr == "" {
		return healthcheck.Config{}, errors.New("ID environment variable is required (unique identifier for this watchdog instance)")
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return healthcheck.Config{}, errors.New("ID must be a number")
	}

	return healthcheck.Config{
		Id:         uint8(id),
		Nodes:      nodes,
		HealthPort: port,
		BullyPort:  bullyPort,
		Interval:   interval,
		Timeout:    timeout,
		MaxRetries: maxRetries,
		Startup:    startup,
		Amount:     uint8(n),
	}, nil
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
