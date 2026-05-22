package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"tp-grupal-distribuidos/internal/join"
)

func splitKeys(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadConfig() (join.JoinConfig, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	leftEofs, _ := strconv.Atoi(os.Getenv("LEFT_EOFS_EXPECTED"))
	rightEofs, _ := strconv.Atoi(os.Getenv("RIGHT_EOFS_EXPECTED"))

	return join.JoinConfig{
		MomHost:            momHost,
		MomPort:            momPort,
		InputExchange:      os.Getenv("INPUT_EXCHANGE"),
		InputQueue:         os.Getenv("INPUT_QUEUE"),
		InputRoutingKeys:   splitKeys(os.Getenv("INPUT_ROUTING_KEYS")),
		LeftInputExchange:  os.Getenv("LEFT_INPUT_EXCHANGE"),
		LeftInputQueue:     os.Getenv("LEFT_INPUT_QUEUE"),
		LeftRoutingKeys:    splitKeys(os.Getenv("LEFT_INPUT_ROUTING_KEYS")),
		RightInputExchange: os.Getenv("RIGHT_INPUT_EXCHANGE"),
		RightInputQueue:    os.Getenv("RIGHT_INPUT_QUEUE"),
		RightRoutingKeys:   splitKeys(os.Getenv("RIGHT_INPUT_ROUTING_KEYS")),
		OutputExchange:     outputExchange,
		OutputQueue:        os.Getenv("OUTPUT_QUEUE"),
		OutputRoutingKeys:  splitKeys(os.Getenv("OUTPUT_ROUTING_KEYS")),
		LeftEofsExpected:   leftEofs,
		RightEofsExpected:  rightEofs,
	}, nil
}
