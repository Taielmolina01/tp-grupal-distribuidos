package config

import (
	"errors"
	"os"
	"strconv"
)

func ParseSenderTrackerCapacity() (int, error) {
	value := os.Getenv("SENDER_TRACKER_CAPACITY")
	if value == "" {
		return 0, errors.New("SENDER_TRACKER_CAPACITY environment variable is required")
	}
	capacity, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New("SENDER_TRACKER_CAPACITY must be an integer")
	}
	if capacity <= 0 {
		return 0, errors.New("SENDER_TRACKER_CAPACITY must be greater than 0")
	}
	return capacity, nil
}
