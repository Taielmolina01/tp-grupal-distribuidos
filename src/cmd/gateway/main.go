package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	commonconfig "tp-grupal-distribuidos/internal/common/config"
	"tp-grupal-distribuidos/internal/common/pinger"
	"tp-grupal-distribuidos/internal/common/shard"
	"tp-grupal-distribuidos/internal/common/splitter"
	"tp-grupal-distribuidos/internal/gateway"
)

const QUEUES_SEPARATOR = ","

func loadConfig() (gateway.GatewayConfig, error) {
	accountsClusterPrefix := os.Getenv("ACCOUNTS_CLUSTER_PREFIX")
	if accountsClusterPrefix == "" {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_CLUSTER_PREFIX environment variable is required")
	}

	accountsClusterAmount, err := strconv.Atoi(os.Getenv("ACCOUNTS_CLUSTER_AMOUNT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("ACCOUNTS_CLUSTER_AMOUNT environment variable is required and must be a number")
	}

	transfersClusters, err := loadTransfersClusters()
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	resultsQueue := os.Getenv("RESULTS_QUEUE")
	if resultsQueue == "" {
		return gateway.GatewayConfig{}, errors.New("RESULTS_QUEUE environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return gateway.GatewayConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	maxBatchSize := 100
	if v := os.Getenv("MAX_BATCH_SIZE"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return gateway.GatewayConfig{}, errors.New("MAX_BATCH_SIZE must be an integer")
		}
		maxBatchSize = parsed
	}

	queryEOFsStr := os.Getenv("QUERY_EOFS_EXPECTED")
	if queryEOFsStr == "" {
		return gateway.GatewayConfig{}, errors.New("QUERY_EOFS_EXPECTED environment variable is required")
	}
	queryEOFsExpected := map[uint8]int{}
	for pair := range strings.SplitSeq(queryEOFsStr, QUEUES_SEPARATOR) {
		pair = strings.TrimSpace(pair)
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid QUERY_EOFS_EXPECTED entry: %q (expected format queryID:count)", pair)
		}
		qid, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 8)
		if err != nil {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid query id in QUERY_EOFS_EXPECTED: %q", parts[0])
		}
		count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || count <= 0 {
			return gateway.GatewayConfig{}, fmt.Errorf("invalid EOF count in QUERY_EOFS_EXPECTED (must be > 0): %q", parts[1])
		}
		queryEOFsExpected[uint8(qid)] = count
	}
	if len(queryEOFsExpected) == 0 {
		return gateway.GatewayConfig{}, errors.New("QUERY_EOFS_EXPECTED must define at least one query")
	}

	sessionStorePath := os.Getenv("SESSION_STORE_PATH")
	if sessionStorePath == "" {
		sessionStorePath = "/data/gateway_sessions.bin"
	}

	var seqCheckpointEvery uint64 = 100
	if v := os.Getenv("SEQ_CHECKPOINT_EVERY"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil || parsed == 0 {
			return gateway.GatewayConfig{}, errors.New("SEQ_CHECKPOINT_EVERY must be a positive integer")
		}
		seqCheckpointEvery = parsed
	}

	clientTimeout := 30 * time.Second
	if v := os.Getenv("CLIENT_TIMEOUT_MS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed <= 0 {
			return gateway.GatewayConfig{}, errors.New("CLIENT_TIMEOUT_MS must be a positive integer")
		}
		clientTimeout = time.Duration(parsed) * time.Millisecond
	}

	reaperInterval := 5 * time.Second
	if v := os.Getenv("REAPER_INTERVAL_MS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed <= 0 {
			return gateway.GatewayConfig{}, errors.New("REAPER_INTERVAL_MS must be a positive integer")
		}
		reaperInterval = time.Duration(parsed) * time.Millisecond
	}

	resultsBatchFlushInterval := 500 * time.Millisecond
	if v := os.Getenv("PERSIST_FLUSH_INTERVAL"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil || parsed <= 0 {
			return gateway.GatewayConfig{}, errors.New("PERSIST_FLUSH_INTERVAL must be a positive duration")
		}
		resultsBatchFlushInterval = parsed
	}

	senderTrackerCapacity, err := commonconfig.ParseSenderTrackerCapacity()
	if err != nil {
		return gateway.GatewayConfig{}, err
	}

	return gateway.GatewayConfig{
		TransfersClusters:         transfersClusters,
		ResultsQueue:              resultsQueue,
		ServerHost:                serverHost,
		ServerPort:                serverPort,
		MomHost:                   momHost,
		MomPort:                   momPort,
		MaxBatchSize:              maxBatchSize,
		QueryEOFsExpected:         queryEOFsExpected,
		SessionStorePath:          sessionStorePath,
		SeqCheckpointEvery:        seqCheckpointEvery,
		ClientTimeout:             clientTimeout,
		ReaperInterval:            reaperInterval,
		ResultsBatchFlushInterval: resultsBatchFlushInterval,
		AccountsClusterPrefix:     accountsClusterPrefix,
		AccountsClusterAmount:     accountsClusterAmount,
		SenderTrackerCapacity:     uint64(senderTrackerCapacity),
	}, nil
}

func loadTransfersClusters() ([]shard.ClusterConfig, error) {
	clustersStr := os.Getenv("TRANSFERS_CLUSTERS")
	if clustersStr == "" {
		return nil, errors.New("TRANSFERS_CLUSTERS environment variable is required")
	}
	parts := splitter.Split(clustersStr, QUEUES_SEPARATOR)
	clusters := make([]shard.ClusterConfig, 0, len(parts))
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid TRANSFERS_CLUSTERS entry %q (expected prefix:N)", part)
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid node count in TRANSFERS_CLUSTERS entry %q: %w", part, err)
		}
		clusters = append(clusters, shard.ClusterConfig{
			Prefix:    strings.TrimSpace(kv[0]),
			NodeCount: n,
		})
	}
	return clusters, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While initializing gateway", "err", err)
		return 1
	}

	healthPinger := pinger.Serve(":" + pinger.DefaultPort)
	defer healthPinger.Close()

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
