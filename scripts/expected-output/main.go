package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"tp-grupal-distribuidos/scripts/expected-output/internal"
)

var (
	inputDir        = envOrString("INPUT_DIR", "./input")
	expectedDir     = envOrString("EXPECTED_DIR", "./expected_output")
	outputDir       = envOrString("OUTPUT_DIR", "./output")
	amountOfClients = envOrInt("N_CLIENTS", 1)
)

func envOrString(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envOrInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: expected-output build | verify")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "build":
		err = internal.Build(inputDir, expectedDir, outputDir, amountOfClients)
	case "verify":
		err = internal.Verify(expectedDir, outputDir, amountOfClients)
	default:
		fmt.Fprintf(os.Stderr, "subcomando desconocido: %s (usa 'build' o 'verify')\n", os.Args[1])
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}
