package filter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/queryresult"
	"tp-grupal-distribuidos/internal/common/transfer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/filterandsplitter"
)

const IGNORED_CURRENCY = "Bitcoin"

func CreateCurrencyFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidCurrency(t, config)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
	)
}

func CreateAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return t.AmountPaid < config.Amount
		},
		func(t transfer.Transfer) queryresult.Query1Result {
			return queryresult.Query1Result{
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				ToBank:      t.ToBank,
				ToAccount:   t.ToBankAccount,
				Amount:      t.AmountPaid,
			}
		},
	)
}

func CreateDateRangeFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
	)
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidPaymentMethod(t, config) && !t.Timestamp.Before(config.StartDateRange) && t.Timestamp.Before(config.EndDateRange)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
	)
}

func CreateFilterAndSplitter(config filterandsplitter.FilterAndSplitterConfig) (worker.Worker, error) {
	return filterandsplitter.NewFilterAndSplitter(
		config,
	)
}

func CreateBankDistinctFilter(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(
		config,
		func(ac1 account.Account, ac2 account.Account) bool {
			return ac1.BankId == ac2.BankId
		},
		func(ac account.Account) string {
			return normalizer.NormalizeBankID(ac.BankId)
		},
		func(t account.Account) string {
			return normalizer.NormalizeBankID(t.BankId)
		},
	)
}

func CreateConvertedAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
		func(t transfer.Transfer, f fetcherresponse.FetcherResponse) bool {
			return t.AmountPaid/f.Rate < config.Amount
		}, // compareFunc
		func(f fetcherresponse.FetcherResponse) string {
			return f.Date
		}, // leftKeyFunc
		func(f fetcherresponse.FetcherResponse) string {
			return f.Quote
		}, // leftSecondKeyFunc
		func(f fetcherresponse.FetcherResponse) float64 {
			return f.Rate
		}, // leftValueFunc
		func(t transfer.Transfer) string {
			return t.Timestamp.Format(DATE_LAYOUT)
		}, // rightKeyFunc
		func(t transfer.Transfer) string {
			return t.ReceivingCurrency
		}, // rightSecondKeyFunc
		func(t transfer.Transfer) float64 {
			return t.AmountPaid
		}, // rightValueFunc
		func(t transfer.Transfer, rate float64) fetcherresponse.FetcherResponse {
			return fetcherresponse.FetcherResponse{
				Date:  t.Timestamp.Format(DATE_LAYOUT),
				Quote: t.ReceivingCurrency,
				Rate:  rate,
			}
		}, // conversionFunc
		func(t transfer.Transfer, clientID int) string {
			fields := []string{}
			fields = append(fields, t.Timestamp.Format(DATE_LAYOUT))
			fields = append(fields, t.FromBank)
			fields = append(fields, t.FromBankAccount)
			fields = append(fields, t.ToBank)
			fields = append(fields, t.ToBankAccount)
			fields = append(fields, fmt.Sprintf("%f", t.AmountReceived))
			fields = append(fields, t.ReceivingCurrency)
			fields = append(fields, fmt.Sprintf("%f", t.AmountPaid))
			fields = append(fields, t.PaymentCurrency)
			fields = append(fields, t.PaymentFormat)
			fields = append(fields, fmt.Sprintf("%t", t.IsLaundering))
			fields = append(fields, fmt.Sprintf("%d", clientID))
			return strings.Join(fields, ",")
		}, // toSveFunc
		func(line string) (transfer.Transfer, int, error) {
			columns := strings.Split(line, ",")
			if len(columns) < 11 {
				return transfer.Transfer{}, -1, fmt.Errorf("invalid line format")
			}
			timestamp, err := time.Parse(DATE_LAYOUT, columns[0])
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing timestamp: %w", err)
			}
			amountReceived, err := strconv.ParseFloat(columns[5], 64)
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing amount received: %w", err)
			}
			amountPaid, err := strconv.ParseFloat(columns[7], 64)
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing amount paid: %w", err)
			}
			clientId, err := strconv.Atoi(columns[11])
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing client ID: %w", err)
			}
			return transfer.Transfer{
				Timestamp:         timestamp,
				FromBank:          columns[1],
				FromBankAccount:   columns[2],
				ToBank:            columns[3],
				ToBankAccount:     columns[4],
				AmountReceived:    amountReceived,
				ReceivingCurrency: columns[6],
				AmountPaid:        amountPaid,
				PaymentCurrency:   columns[8],
				PaymentFormat:     columns[9],
				IsLaundering:      columns[10] == "true",
			}, clientId, nil
		}, // fromSaveFunc
		func(t transfer.Transfer) bool {
			return t.PaymentCurrency == IGNORED_CURRENCY
		}, // ignoreFunc
	)
}
