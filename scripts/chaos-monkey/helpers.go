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

func ListServices(path string) ([]string, error) {
	data, _ := os.ReadFile(path)
	var c Compose
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(c.Services))
	for name := range c.Services {
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
	if err := exec.Command("docker", "stop", node).Run(); err != nil {
		return err
	}
	return exec.Command("docker", "restart", node).Run()
}
