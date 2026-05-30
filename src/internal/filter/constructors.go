package filter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/fetcherresponse"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
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
		transfer.ProjectAfterCurrency,
		records.TransferCodec,
		records.TransferAfterCurrencyCodec,
	)
}

func CreateAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.TransferAfterCurrency) bool {
			return t.AmountPaid < config.Amount
		},
		func(t transfer.TransferAfterCurrency) queryresult.Query1Result {
			return queryresult.Query1Result{
				FromBank:    t.FromBank,
				FromAccount: t.FromBankAccount,
				ToBank:      t.ToBank,
				ToAccount:   t.ToBankAccount,
				Amount:      t.AmountPaid,
			}
		},
		records.TransferAfterCurrencyCodec,
		records.Query1ResultCodec,
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
		records.TransferCodec,
		records.TransferCodec,
	)
}

func CreateDateRangeAndPaymentMethod(config FilterConfig) (worker.Worker, error) {
	return newFilter(
		config,
		func(t transfer.Transfer) bool {
			return isValidPaymentMethod(t, config) && !t.Timestamp.Before(config.StartDateRange) && t.Timestamp.Before(config.EndDateRange)
		},
		transfer.ProjectForQ5Filter,
		records.TransferCodec,
		records.TransferForQ5FilterCodec,
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
		records.AccountCodec,
	)
}

func CreateConvertedAmountFilter(config FilterConfig) (worker.Worker, error) {
	return newConvertedAmountFilter(
		config,
		func(t transfer.TransferForQ5Filter, f fetcherresponse.FetcherResponse) bool {
			return t.AmountPaid/f.Rate < config.Amount
		},
		func(f fetcherresponse.FetcherResponse) string { return f.Date },
		func(f fetcherresponse.FetcherResponse) string { return f.Quote },
		func(f fetcherresponse.FetcherResponse) float64 { return f.Rate },
		func(t transfer.TransferForQ5Filter) string {
			return t.Timestamp.Format(DATE_LAYOUT)
		},
		func(t transfer.TransferForQ5Filter) string { return t.Currency },
		func(t transfer.TransferForQ5Filter) float64 { return t.AmountPaid },
		func(t transfer.TransferForQ5Filter, rate float64) fetcherresponse.FetcherResponse {
			return fetcherresponse.FetcherResponse{
				Date:  t.Timestamp.Format(DATE_LAYOUT),
				Quote: t.Currency,
				Rate:  rate,
			}
		},
		func(t transfer.TransferForQ5Filter, clientID int) string {
			return strings.Join([]string{
				t.Timestamp.Format(time.RFC3339),
				t.Currency,
				strconv.FormatFloat(t.AmountPaid, 'f', -1, 64),
				strconv.Itoa(clientID),
			}, ",")
		},
		func(line string) (transfer.TransferForQ5Filter, int, error) {
			columns := strings.Split(line, ",")
			if len(columns) < 4 {
				return transfer.TransferForQ5Filter{}, -1, fmt.Errorf("invalid line format")
			}
			timestamp, err := time.Parse(time.RFC3339, columns[0])
			if err != nil {
				return transfer.TransferForQ5Filter{}, -1, fmt.Errorf("error while parsing timestamp: %w", err)
			}
			amountPaid, err := strconv.ParseFloat(columns[2], 64)
			if err != nil {
				return transfer.TransferForQ5Filter{}, -1, fmt.Errorf("error while parsing amount paid: %w", err)
			}
			clientId, err := strconv.Atoi(columns[3])
			if err != nil {
				return transfer.TransferForQ5Filter{}, -1, fmt.Errorf("error while parsing client ID: %w", err)
			}
			return transfer.TransferForQ5Filter{
				Timestamp:  timestamp,
				Currency:   columns[1],
				AmountPaid: amountPaid,
			}, clientId, nil
		},
		func(t transfer.TransferForQ5Filter) bool {
			return t.Currency == IGNORED_CURRENCY
		},
		func() transfer.FinalTransferForQ5 {
			return transfer.ProjectForQ5Final()
		},
		records.FetcherResponseCodec,
		records.TransferForQ5FilterCodec,
		records.FinalTransferForQ5Codec,
	)
}
