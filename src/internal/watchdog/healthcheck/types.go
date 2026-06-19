package healthcheck

import (
	"context"
	"sync"
	"time"
	"tp-grupal-distribuidos/internal/common/bully"
)

const _WATCHDOG_PREFIX = "watchdog"

type Config struct {
	Id         uint8
	Nodes      []string
	HealthPort string
	BullyPort  string
	Interval   time.Duration
	Timeout    time.Duration
	MaxRetries int
	Startup    time.Duration
	Amount     uint8
}

type BullyInfo struct {
	LeaderId uint8
}

type Restarter func(node string) error

type HealthChecker struct {
	config             Config
	restart            Restarter
	leaderWaitGroup    sync.WaitGroup
	watchdogsWaitGroup sync.WaitGroup
	bullyInfo          BullyInfo
	mu                 sync.RWMutex
	bully              bully.Bully
	condvarMutex       *sync.Mutex
	condvar            *sync.Cond
	amLeader           bool
	leaderCancel       context.CancelFunc
	watchdogsCancel    context.CancelFunc
	stopped            bool
}
