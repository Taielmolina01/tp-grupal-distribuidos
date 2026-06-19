package healthcheck

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"tp-grupal-distribuidos/internal/common/bully"
	"tp-grupal-distribuidos/internal/common/pinger"
)

func DockerRestart(node string) error {
	err := exec.Command("docker", "stop", node).Run()
	if err != nil {
		return err
	}
	return exec.Command("docker", "restart", node).Run()
}

func New(config Config, bully bully.Bully) *HealthChecker {
	condvarMutex := sync.Mutex{}
	return &HealthChecker{
		config:       config,
		restart:      DockerRestart,
		bully:        bully,
		condvarMutex: &condvarMutex,
		condvar:      sync.NewCond(&condvarMutex),
	}
}

func (h *HealthChecker) Start() {
	slog.Info("Health checker started", "nodes", len(h.config.Nodes), "interval", h.config.Interval)

	wdCtx, wdCancel := context.WithCancel(context.Background())
	h.watchdogsCancel = wdCancel

	// Start monitoring only watchdog nodes with a dedicated context.
	for _, node := range h.config.Nodes {
		if node == fmt.Sprintf("watchdog_%d", h.config.Id) {
			continue
		}
		if len(node) >= 8 && node[:8] == _WATCHDOG_PREFIX {
			h.watchdogsWaitGroup.Add(1)
			go h.monitor(node, wdCtx, &h.watchdogsWaitGroup)
		}
	}

	for {
		h.condvarMutex.Lock()
		for !h.amLeader && !h.stopped {
			h.condvar.Wait()
		}
		if h.stopped {
			h.condvarMutex.Unlock()
			return
		}

		// I become the leader so I start monitoring all nodes with a dedicated context.
		ctx, cancel := context.WithCancel(context.Background())
		h.leaderCancel = cancel
		h.condvarMutex.Unlock()

		for _, node := range h.config.Nodes {
			if node == fmt.Sprintf("watchdog_%d", h.config.Id) {
				continue
			}
			h.leaderWaitGroup.Add(1)
			go h.monitor(node, ctx, &h.leaderWaitGroup)
		}

		h.leaderWaitGroup.Wait()
	}
}

func (h *HealthChecker) Stop() {
	h.condvarMutex.Lock()
	if h.leaderCancel != nil {
		h.leaderCancel()
	}
	h.stopped = true
	h.condvar.Broadcast()
	h.condvarMutex.Unlock()

	h.leaderWaitGroup.Wait()

	if h.watchdogsCancel != nil {
		h.watchdogsCancel()
	}
	h.watchdogsWaitGroup.Wait()

	slog.Info("Health checker stopped")
}

func (h *HealthChecker) monitor(node string, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	addr := net.JoinHostPort(node, h.config.HealthPort)
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	retries := 0
	lastRestart := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := pinger.Check(addr, h.config.Timeout); err != nil {
				retries++
				slog.Debug("Ping failed", "node", node, "retries", retries, "err", err)
			} else {
				retries = 0
				continue
			}

			if retries >= h.config.MaxRetries {
				h.handleDeadNode(node, &lastRestart)
				retries = 0
			}
		}
	}
}

func (h *HealthChecker) handleDeadNode(node string, lastRestart *time.Time) {
	h.mu.RLock()
	leaderId := h.bullyInfo.LeaderId
	h.mu.RUnlock()
	slog.Info("node info", "node", node)

	if time.Since(*lastRestart) < h.config.Startup {
		slog.Info("Node unresponsive but still within startup grace, not restarting", "node", node)
		return
	}

	if len(node) >= 8 && node[:8] == _WATCHDOG_PREFIX && len(node) >= 10 && node[9:] == fmt.Sprintf("%d", leaderId) {
		h.bully.StartElection()
		slog.Info("Leader is down, not restarting.", "leaderId", leaderId)
	}

	if leaderId != h.config.Id {
		slog.Info("Skipping restarting node, I'm not the leader", "node", h.config.Id)
		return
	}

	slog.Info("Node declared dead, restarting", "node", node)
	if err := h.restart(node); err != nil {
		slog.Error("While restarting node", "node", node, "err", err)
		return
	}
	*lastRestart = time.Now()
	slog.Info("Node restarted", "node", node)
	if len(node) >= 8 && node[:8] == _WATCHDOG_PREFIX && len(node) >= 10 {
		id, err := strconv.Atoi(node[9:])
		if err != nil {
			slog.Error("Failed to parse node ID", "node", node, "err", err)
			return
		}
		h.bully.SendNodeIsAliveMessage(id)
	}
}

func (h *HealthChecker) UpdateBullyInfo(info BullyInfo) {
	h.mu.Lock()
	h.bullyInfo = info
	h.mu.Unlock()

	h.condvarMutex.Lock()
	h.condvar.Broadcast()
	h.amLeader = info.LeaderId == h.config.Id
	if !h.amLeader && h.leaderCancel != nil {
		h.leaderCancel()
	}
	h.condvarMutex.Unlock()

	slog.Info("Bully info updated", "leaderId", info.LeaderId)
}
