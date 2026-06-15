package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"tp-grupal-distribuidos/internal/seqstorenode"
)

func loadConfig() (seqstorenode.Config, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return seqstorenode.Config{}, errors.New("MOM_HOST is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return seqstorenode.Config{}, errors.New("MOM_PORT is required and must be a number")
	}

	requestQueue := os.Getenv("REQUEST_QUEUE")
	if requestQueue == "" {
		return seqstorenode.Config{}, errors.New("REQUEST_QUEUE is required")
	}

	capacity := uint64(10_000_000)
	if v := os.Getenv("SEQ_CAPACITY"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return seqstorenode.Config{}, errors.New("SEQ_CAPACITY must be a positive integer")
		}
		capacity = parsed
	}

	persistPath := os.Getenv("PERSIST_PATH")
	if persistPath == "" {
		return seqstorenode.Config{}, errors.New("PERSIST_PATH is required")
	}

	return seqstorenode.Config{
		MomHost:      momHost,
		MomPort:      momPort,
		RequestQueue: requestQueue,
		Capacity:     capacity,
		PersistPath:  persistPath,
	}, nil
}

func run() int {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("loading config", "err", err)
		return 1
	}

	node, err := seqstorenode.New(cfg)
	if err != nil {
		slog.Error("initializing seqstorenode", "err", err)
		return 1
	}

	go node.HandleSignals()
	node.Run()
	return 0
}

func main() {
	os.Exit(run())
}
