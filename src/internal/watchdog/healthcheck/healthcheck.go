package healthcheck

import (
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"time"

	"tp-grupal-distribuidos/internal/common/bully"
	"tp-grupal-distribuidos/internal/common/pinger"
)

func DockerRestart(node string) error {
	return exec.Command("docker", "restart", node).Run()
}

func New(config Config, bully bully.Bully) *HealthChecker {
	return &HealthChecker{
		config:  config,
		restart: DockerRestart,
		stop:    make(chan struct{}),
		bully:   bully,
	}
}

func (h *HealthChecker) Start() {
	slog.Info("Health checker started", "nodes", len(h.config.Nodes), "interval", h.config.Interval)
	for _, node := range h.config.Nodes {
		if node == fmt.Sprintf("watchdog_%d", h.config.Id) {
			continue
		}
		h.wg.Add(1)
		go h.monitor(node)
	}
}

func (h *HealthChecker) Stop() {
	close(h.stop)
	h.wg.Wait()
	slog.Info("Health checker stopped")
}

func (h *HealthChecker) monitor(node string) {
	defer h.wg.Done()

	addr := net.JoinHostPort(node, h.config.HealthPort)
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	retries := 0
	lastRestart := time.Now()

	for {
		select {
		case <-h.stop:
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
}

func (h *HealthChecker) UpdateBullyInfo(info BullyInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.bullyInfo = info
	slog.Info("Bully info updated", "leaderId", info.LeaderId)
}
