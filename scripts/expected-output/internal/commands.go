package internal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func Build(inputDir, expectedDir, outputDir string, amountOfClients int) error {
	if err := os.MkdirAll(expectedDir, 0o755); err != nil {
		return err
	}

	log.Printf("Fetching rates from %s", frankfurterURL)
	rates, err := fetchRates()
	if err != nil {
		return fmt.Errorf("no se pudieron obtener las cotizaciones: %w", err)
	}

	for i := range amountOfClients {
		log.Printf("Procesando dataset (rep cliente %d)", i)
		accounts, err := loadAccounts(inputDir, i)
		if err != nil {
			return err
		}
		prefix := filepath.Join(expectedDir, fmt.Sprintf("expected_%d", i))
		if err := getQuery1Result(inputDir, i, prefix+"_1.csv"); err != nil {
			return err
		}
		if err := getQuery2Result(inputDir, i, accounts, prefix+"_2.csv"); err != nil {
			return err
		}
		if err := getQuery3Result(inputDir, i, prefix+"_3.csv"); err != nil {
			return err
		}
		if err := getQuery4Result(inputDir, i, prefix+"_4.csv"); err != nil {
			return err
		}
		if err := getQuery5Result(inputDir, i, rates, prefix+"_5.csv"); err != nil {
			return err
		}
	}
	return nil
}

func Verify(expectedDir, outputDir string, amountOfClients int) error {
	allOk := true
	for clientID := 0; clientID < amountOfClients; clientID++ {
		for q := 1; q < 6; q++ {
			tag := fmt.Sprintf("[client %d q%d]", clientID, q)
			outputPath := filepath.Join(outputDir, fmt.Sprintf("output_%d_%d.csv", clientID, q))
			expectedPath := filepath.Join(expectedDir, fmt.Sprintf("expected_%d_%d.csv", clientID, q))
			if _, err := os.Stat(outputPath); err != nil {
				fmt.Println(tag, "MISSING output", outputPath)
				allOk = false
				continue
			}
			if _, err := os.Stat(expectedPath); err != nil {
				fmt.Println(tag, "MISSING expected", expectedPath)
				allOk = false
				continue
			}
			hasDiff, err := diffSortedFiles(tag, outputPath, expectedPath, os.Stdout)
			if err != nil {
				return err
			}
			if !hasDiff {
				fmt.Println(tag, "OK")
			} else {
				allOk = false
			}
		}
	}
	if allOk {
		fmt.Println("verify-output OK")
		return nil
	}
	fmt.Println("verify-output FAILED")
	os.Exit(1)
	return nil
}
