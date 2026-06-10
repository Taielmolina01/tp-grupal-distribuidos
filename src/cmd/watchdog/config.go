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

	port := os.Getenv("PING_PORT")
	if port == "" {
		port = pinger.DefaultPort
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

	return healthcheck.Config{
		Nodes:      nodes,
		Port:       port,
		Interval:   interval,
		Timeout:    timeout,
		MaxRetries: maxRetries,
		Startup:    startup,
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
