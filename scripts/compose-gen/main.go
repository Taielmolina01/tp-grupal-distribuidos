package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const (
	defaultConfigFile = "compose-config.yaml"
	defaultOutputFile = "../../docker-compose.yaml"
)

func main() {
	configPath := flag.String("config", defaultConfigFile, "replica config file (.yaml)")
	outputPath := flag.String("output", defaultOutputFile, "output docker-compose file")
	flag.Parse()

	cleanConfig := filepath.Clean(*configPath)
	cleanOutput := filepath.Clean(*outputPath)

	cfg, err := loadConfig(cleanConfig)
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	compose := generateCompose(cfg)

	if err := os.WriteFile(cleanOutput, []byte(compose), 0644); err != nil {
		log.Fatalf("error writing output: %v", err)
	}

	fmt.Printf("✓ docker-compose generated at: %s\n\n", cleanOutput)
	fmt.Printf("Applied configuration:\n")
	fmt.Printf("  clients:                     %d\n", cfg.Clients)
	fmt.Printf("  max_batch_kb:                %d\n", cfg.MaxBatchKB)
	fmt.Printf("  filter_currency (Q1):        %d\n", cfg.FilterCurrency)
	fmt.Printf("  filter_amount (Q1):          %d\n", cfg.FilterAmount)
	fmt.Printf("  reducer_q2 (Q2):             %d\n", cfg.ReducerQ2)
	fmt.Printf("  join_q2 (Q2):                %d\n", cfg.JoinQ2)
	fmt.Printf("  filter_and_splitter_q4 (Q4): %d\n", cfg.FilterAndSplitterQ4)
	fmt.Printf("  join_accounts_q4 (Q4):       %d\n", cfg.JoinAccountsQ4)
	fmt.Printf("  acum_accounts_q4 (Q4):       %d\n", cfg.AcumAccountsQ4)
	fmt.Printf("  filter_account_seen_q4 (Q4): %d\n", cfg.FilterAccountSeenQ4)
	fmt.Printf("  filter_range_q3 (Q3):        %d\n", cfg.FilterRangeQ3)
	fmt.Printf("  sum_q3 (Q3):                 %d\n", cfg.SumQ3)
	fmt.Printf("  aggregate_q3 (Q3):           %d\n", cfg.AggregateQ3)
	fmt.Printf("  average_filter_q3 (Q3):      %d\n", cfg.AverageFilterQ3)
	fmt.Printf("  filter_date_and_payment (Q5):%d\n", cfg.FilterDateAndPayment)
	fmt.Printf("  filter_amt_q5 (Q5):          %d\n", cfg.FilterAmtQ5)
	fmt.Printf("  filter_bank_id_already_seen: %d\n", cfg.FilterBankIdAlreadySeen)
}
