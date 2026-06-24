package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

const DEFAULT_YAML_PATH = "docker-compose.yaml"

func main() {
	yamlPath := os.Getenv("YAML_PATH")
	if yamlPath == "" {
		yamlPath = DEFAULT_YAML_PATH
	}
	roundsStr := os.Getenv("ROUNDS")
	rounds, err := strconv.Atoi(roundsStr)
	if err != nil || rounds <= 0 {
		log.Fatalf("Invalid ROUNDS value: %v", err)
	}
	sleepRoundStr := os.Getenv("SLEEP_ROUND")
	sleepRound, err := strconv.Atoi(sleepRoundStr)
	if err != nil || sleepRound <= 0 {
		log.Fatalf("Invalid SLEEP_ROUND value: %v", err)
	}
	amountOfContainersToRestartStr := os.Getenv("AMOUNT_OF_CONTAINERS_TO_RESTART")
	amountOfContainersToRestart, err := strconv.Atoi(amountOfContainersToRestartStr)
	if err != nil || amountOfContainersToRestart <= 0 {
		log.Fatalf("Invalid AMOUNT_OF_CONTAINERS_TO_RESTART value: %v", err)
	}
	containers, err := ListServices(yamlPath)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < rounds; i++ {
		log.Printf("Round %d/%d", i+1, rounds)
		restartContainers(containers, amountOfContainersToRestart)
		time.Sleep(time.Duration(sleepRound) * time.Second)
	}
}
