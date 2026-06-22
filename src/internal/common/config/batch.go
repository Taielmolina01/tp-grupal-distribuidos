package config

import (
	"errors"
	"os"
	"strconv"
)

const bytesPerKB = 1024

func ParseMaxBatchBytes() (int, error) {
	value := os.Getenv("MAX_BATCH_KB")
	if value == "" {
		return 0, errors.New("MAX_BATCH_KB environment variable is required")
	}
	maxBatchKB, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New("MAX_BATCH_KB must be an integer")
	}
	if maxBatchKB <= 0 {
		return 0, errors.New("MAX_BATCH_KB must be greater than 0")
	}
	return maxBatchKB * bytesPerKB, nil
}
