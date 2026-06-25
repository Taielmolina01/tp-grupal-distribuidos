package main

import (
	"log"
	"math/rand"
	"os"
	"os/exec"
	"time"

	"go.yaml.in/yaml/v3"
)

type Compose struct {
	Services map[string]any `yaml:"services"`
}

const (
	_RABBITMQ_SERVICE = "rabbitmq"
	_CLIENT_SHAPE     = "client_"
)

func ListServices(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Compose
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(c.Services))
	for name := range c.Services {
		if name == _RABBITMQ_SERVICE || len(name) >= len(_CLIENT_SHAPE) && name[:len(_CLIENT_SHAPE)] == _CLIENT_SHAPE {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func restartContainers(containers []string, amount int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range amount {
		container := containers[rng.Intn(len(containers))]
		log.Printf("Restarting container: %s", container)
		if err := DockerRestart(container); err != nil {
			log.Printf("Error restarting container %s: %v", container, err)
		}
	}
}

func DockerRestart(node string) error {
	return exec.Command("docker", "restart", node).Run()
}
