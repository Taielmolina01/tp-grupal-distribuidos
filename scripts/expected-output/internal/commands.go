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

	for i := 0; i < amountOfClients; i++ {
		log.Printf("Procesando dataset (rep cliente %d)", i)
		transactions, err := loadTransactions(inputDir, i)
		if err != nil {
			return err
		}
		accounts, err := loadAccounts(inputDir, i)
		if err != nil {
			return err
		}
		prefix := filepath.Join(expectedDir, fmt.Sprintf("expected_%d", i))
		if err := getQuery1Result(transactions, prefix+"_1.csv"); err != nil {
			return err
		}
		if err := getQuery2Result(transactions, accounts, prefix+"_2.csv"); err != nil {
			return err
		}
		if err := getQuery3Result(transactions, prefix+"_3.csv"); err != nil {
			return err
		}
		if err := getQuery4Result(transactions, prefix+"_4.csv"); err != nil {
			return err
		}
		if err := getQuery5Result(transactions, rates, prefix+"_5.csv"); err != nil {
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
			outLines, err := sortedLines(outputPath)
			if err != nil {
				return err
			}
			expLines, err := sortedLines(expectedPath)
			if err != nil {
				return err
			}
			diffs := diffSorted(outLines, expLines)
			if len(diffs) == 0 {
				fmt.Println(tag, "OK")
			} else {
				fmt.Println(tag, "DIFF:")
				for _, d := range diffs {
					fmt.Println(d)
				}
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
