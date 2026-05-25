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
)

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
			return t.AmountPaid > config.Amount
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
			return isValidPaymentMethod(t, config) && t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
	)
}

func CreateCountAndFilter(config CountAndFilterConfig) (worker.Worker, error) {
	return newCountAndFilter[transfer.Transfer](config)
}

func CreateFilterAndSplitter(config FilterConfig) (worker.Worker, error) {
	return newFilterAndSplitter(
		config,
		func(t transfer.Transfer) bool {
			return t.Timestamp.Before(config.EndDateRange) && t.Timestamp.After(config.StartDateRange)
		},
		func(t transfer.Transfer) (t1 transfer.SplittedTransfer, t2 transfer.SplittedTransfer) {
			// TO DO: rellenar esto
			return transfer.SplittedTransfer{}, transfer.SplittedTransfer{}
		},
	)
}

func CreateTransferDistinctFilter(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(
		config,
		func(t1 transfer.Transfer, t2 transfer.Transfer) bool {
			return t1.Equals(t2)
		},
		func(t transfer.Transfer) transfer.Transfer {
			return t
		},
		func(t transfer.Transfer) string {
			return t.FromBank
		},
	)
}

func CreateBankDistinctFilter(config FilterConfig) (worker.Worker, error) {
	return newDistinctFilter(
		config,
		func(ac1 account.Account, ac2 account.Account) bool {
			return ac1.BankId == ac2.BankId
		},
		func(ac account.Account) string {
			return ac.BankId
		},
		func(t account.Account) string {
			return normalizer.NormalizeBankID(t.BankId)
		},
	)
}

func CreateAverageFilter(config FilterConfig) (worker.Worker, error) {
	// Precondiciones:
	// - Primer número: monto de la transferencia cruda
	// - Segundo número: monto promedio de las transferencias
	return newAverageFilter(config, func(n1 float32, n2 float32) bool {
		return n1 <= n2+config.Amount && n1 >= n2-config.Amount
	})
}

func CreateConvertedAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
		func(t transfer.Transfer, f fetcherresponse.FetcherResponse) bool {
			return t.AmountPaid/f.Rate < config.Amount
		},
		func(f fetcherresponse.FetcherResponse) string {
			return f.Date
		},
		func(f fetcherresponse.FetcherResponse) string {
			return f.Quote
		},
		func(f fetcherresponse.FetcherResponse) float32 {
			return f.Rate
		},
		func(t transfer.Transfer) string {
			return t.Timestamp.Format(DATE_LAYOUT)
		},
		func(t transfer.Transfer) string {
			return t.ReceivingCurrency
		},
		func(t transfer.Transfer) float32 {
			return t.AmountPaid
		},
		func(t transfer.Transfer, rate float32) fetcherresponse.FetcherResponse {
			return fetcherresponse.FetcherResponse{
				Date:  t.Timestamp.Format(DATE_LAYOUT),
				Quote: t.ReceivingCurrency,
				Rate:  rate,
			}
		},
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
		},
		func(line string) (transfer.Transfer, int, error) {
			columns := strings.Split(line, ",")
			if len(columns) < 11 {
				return transfer.Transfer{}, -1, fmt.Errorf("invalid line format")
			}
			timestamp, err := time.Parse(DATE_LAYOUT, columns[0])
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing timestamp: %w", err)
			}
			amountReceived, err := strconv.ParseFloat(columns[5], 32)
			if err != nil {
				return transfer.Transfer{}, -1, fmt.Errorf("error while parsing amount received: %w", err)
			}
			amountPaid, err := strconv.ParseFloat(columns[7], 32)
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
				AmountReceived:    float32(amountReceived),
				ReceivingCurrency: columns[6],
				AmountPaid:        float32(amountPaid),
				PaymentCurrency:   columns[8],
				PaymentFormat:     columns[9],
				IsLaundering:      columns[10] == "true",
			}, clientId, nil
		},
	)
}
