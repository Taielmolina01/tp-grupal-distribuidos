package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Clients int `yaml:"clients"`

	MaxBatchKB int `yaml:"max_batch_kb"`

	SenderTrackerCapacity int `yaml:"sender_tracker_capacity"`

	// Query 1-4
	FilterCurrency int `yaml:"filter_currency"`

	// Query 1
	FilterAmount int `yaml:"filter_amount"`

	// Query 2
	ReducerQ2               int `yaml:"reducer_q2"`
	FilterBankIdAlreadySeen int `yaml:"filter_bank_id_already_seen"`
	JoinQ2                  int `yaml:"join_q2"`

	// Query 4
	FilterAndSplitterQ4 int `yaml:"filter_and_splitter_q4"`
	JoinAccountsQ4      int `yaml:"join_accounts_q4"`
	AcumAccountsQ4      int `yaml:"acum_accounts_q4"`
	FilterAccountSeenQ4 int `yaml:"filter_account_seen_q4"`

	// Query 3
	FilterRangeQ3   int `yaml:"filter_range_q3"`
	SumQ3           int `yaml:"sum_q3"`
	AggregateQ3     int `yaml:"aggregate_q3"`
	AverageFilterQ3 int `yaml:"average_filter_q3"`

	// Query 5
	FilterDateAndPayment int `yaml:"filter_date_and_payment"`
	FilterAmtQ5          int `yaml:"filter_amt_q5"`

	Watchdogs int `yaml:"watchdogs"`
}

func loadConfig(path string) (*Config, error) {
	cleanPath := filepath.Clean(path)

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("error reading cfg file %q: %w", cleanPath, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshalling cfg file: %w", err)
	}
	if cfg.MaxBatchKB == 0 {
		cfg.MaxBatchKB = 128
	}
	if cfg.SenderTrackerCapacity == 0 {
		cfg.SenderTrackerCapacity = 10000000
	}

	return &cfg, nil
}
