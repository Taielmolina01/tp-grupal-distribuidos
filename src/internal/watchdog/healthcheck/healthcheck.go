package healthcheck

import (
	"log/slog"
	"net"
	"os/exec"
	"sync"
	"time"

	"tp-grupal-distribuidos/internal/common/pinger"
)

type Restarter func(node string) error

func DockerRestart(node string) error {
	return exec.Command("docker", "restart", node).Run()
}

type Config struct {
	Nodes []string
	Port string
	Interval time.Duration
	Timeout time.Duration
	MaxRetries int
	Startup time.Duration
}

type HealthChecker struct {
	config  Config
	restart Restarter
	stop    chan struct{}
	wg      sync.WaitGroup
}

func New(config Config) *HealthChecker {
	return &HealthChecker{
		config:  config,
		restart: DockerRestart,
		stop:    make(chan struct{}),
	}
}

func (h *HealthChecker) SetRestarter(r Restarter) {
	h.restart = r
}

func (h *HealthChecker) Start() {
	slog.Info("Health checker started", "nodes", len(h.config.Nodes), "interval", h.config.Interval)
	for _, node := range h.config.Nodes {
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

	addr := net.JoinHostPort(node, h.config.Port)
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	retries := 0
	lastRestart := time.Time{}

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
	if time.Since(*lastRestart) < h.config.Startup {
		slog.Info("Node unresponsive but still within startup grace, not restarting", "node", node)
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
