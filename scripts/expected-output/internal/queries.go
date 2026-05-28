package internal

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
)

const (
	_USD_CURRENCY                   = "US Dollar"
	_QUERY1_AMOUNT_THRESHOLD        = 50.0
	_QUERY3_AVG_FRACTION            = 0.01
	_QUERY4_MIN_SCATTER             = 5
	_QUERY5_AMOUNT_THRESHOLD        = 1.0
	_QUERY3_FIRST_RANGE_START_DATE  = "2022/09/01"
	_QUERY3_FIRST_RANGE_END_DATE    = "2022/09/06"
	_QUERY3_SECOND_RANGE_START_DATE = "2022/09/06"
	_QUERY3_SECOND_RANGE_END_DATE   = "2022/09/16"
	_QUERY4_START_DATE              = "2022/09/01"
	_QUERY4_END_DATE                = "2022/09/06"
	_QUERY5_START_DATE              = "2022/09/01"
	_QUERY5_END_DATE                = "2022/09/06"
)

var currencyToISO = map[string]string{
	"Australian Dollar": "AUD",
	"Bitcoin":           "BTC",
	"Brazil Real":       "BRL",
	"Canadian Dollar":   "CAD",
	"Euro":              "EUR",
	"Mexican Peso":      "MXN",
	"Ruble":             "RUB",
	"Rupee":             "INR",
	"Saudi Riyal":       "SAR",
	"Shekel":            "ILS",
	"Swiss Franc":       "CHF",
	"UK Pound":          "GBP",
	"US Dollar":         "USD",
	"Yen":               "JPY",
	"Yuan":              "CNY",
}

var validFormatsQ5 = []string{"Wire", "ACH"}

func getQuery1Result(txs []Transaction, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "To Bank", "Account.1", "Amount Paid")
	for _, t := range txs {
		if t.PaymentCurrency == _USD_CURRENCY && t.AmountPaid < _QUERY1_AMOUNT_THRESHOLD {
			out.row(t.FromBank, t.FromAccount, t.ToBank, t.ToAccount, fmtAmount(t.AmountPaid))
		}
	}
	return out.close()
}

func getQuery2Result(txs []Transaction, accs []Account, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}

	maxes := map[string]Transaction{}
	for _, t := range txs {
		if t.PaymentCurrency != _USD_CURRENCY {
			continue
		}
		cur, ok := maxes[normalizeBank(t.FromBank)]
		if !ok || t.AmountPaid > cur.AmountPaid {
			maxes[normalizeBank(t.FromBank)] = t
		}
	}

	bankName := map[string]string{}
	for _, a := range accs {
		bankName[normalizeBank(a.BankID)] = a.BankName
	}

	keys := make([]string, 0, len(maxes))
	for k := range maxes {
		keys = append(keys, k)
	}

	out.row("From Bank", "Account", "Bank Name", "Amount Paid")
	for _, k := range keys {
		m := maxes[k]
		name, ok := bankName[normalizeBank(k)]
		if !ok {
			continue
		}
		out.row(m.FromBank, m.FromAccount, name, fmtAmount(m.AmountPaid))
	}
	return out.close()
}

func getQuery3Result(txs []Transaction, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}

	type entry struct {
		AmountPaid float64
		Total      int32
	}
	sumAndCounts := map[string]entry{}
	for _, t := range txs {
		if t.PaymentCurrency != _USD_CURRENCY {
			continue
		}
		if t.Timestamp >= _QUERY3_FIRST_RANGE_START_DATE && t.Timestamp <= _QUERY3_FIRST_RANGE_END_DATE {
			oldValue := sumAndCounts[t.PaymentFormat]
			sumAndCounts[t.PaymentFormat] = entry{
				AmountPaid: oldValue.AmountPaid + t.AmountPaid,
				Total:      oldValue.Total + 1,
			}
		}
	}
	avg := map[string]float64{}
	for k, s := range sumAndCounts {
		if s.Total > 0 {
			avg[k] = s.AmountPaid / float64(s.Total)
		}
	}

	out.row("From Bank", "Account", "Payment Format", "Amount Paid")
	for _, t := range txs {
		if t.PaymentCurrency != _USD_CURRENCY ||
			(t.Timestamp < _QUERY3_SECOND_RANGE_START_DATE || t.Timestamp >= _QUERY3_SECOND_RANGE_END_DATE) {
			continue
		}
		a, ok := avg[t.PaymentFormat]
		if !ok {
			continue
		}
		if t.AmountPaid < a*_QUERY3_AVG_FRACTION {
			out.row(t.FromBank, t.FromAccount, t.PaymentFormat, fmtAmount(t.AmountPaid))
		}
	}
	return out.close()
}

func getQuery4Result(txs []Transaction, outPath string) error {
	var p1 []Transaction
	for _, t := range txs {
		if t.PaymentCurrency == _USD_CURRENCY &&
			t.Timestamp >= _QUERY4_START_DATE && t.Timestamp < _QUERY4_END_DATE {
			p1 = append(p1, t)
		}
	}

	left := map[BankAcc]map[BankAcc]bool{}
	right := map[BankAcc]map[BankAcc]bool{}

	type pair struct{ A, C BankAcc }
	middles := map[pair]map[BankAcc]bool{}

	addTo := func(m map[BankAcc]map[BankAcc]bool, k, v BankAcc) bool {
		if m[k] == nil {
			m[k] = map[BankAcc]bool{}
		}
		if m[k][v] {
			return false
		}
		m[k][v] = true
		return true
	}
	addMiddle := func(p pair, mid BankAcc) {
		if middles[p] == nil {
			middles[p] = map[BankAcc]bool{}
		}
		middles[p][mid] = true
	}

	for _, t := range p1 {
		a := BankAcc{t.FromBank, t.FromAccount}
		b := BankAcc{t.ToBank, t.ToAccount}

		if addTo(left, a, b) {
			for x := range right[a] {
				addMiddle(pair{x, b}, a)
			}
		}

		if addTo(right, b, a) {
			for c := range left[b] {
				addMiddle(pair{a, c}, b)
			}
		}
	}

	unique := map[BankAcc]bool{}
	for p, ms := range middles {
		if len(ms) >= _QUERY4_MIN_SCATTER {
			unique[p.A] = true
			unique[p.C] = true
		}
	}

	accs := make([]BankAcc, 0, len(unique))
	for a := range unique {
		accs = append(accs, a)
	}
	sort.Slice(accs, func(i, j int) bool {
		if accs[i].Bank != accs[j].Bank {
			return accs[i].Bank < accs[j].Bank
		}
		return accs[i].Account < accs[j].Account
	})

	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("Bank", "Account")
	for _, a := range accs {
		out.row(a.Bank, a.Account)
	}
	return out.close()
}

func getQuery5Result(txs []Transaction, rates map[string]map[string]float64, outPath string) error {
	count := 0
	for _, t := range txs {
		if !(t.Timestamp >= _QUERY5_START_DATE && t.Timestamp < _QUERY5_END_DATE) ||
			!slices.Contains(validFormatsQ5, t.PaymentFormat) ||
			t.ReceivingCurrency == "Bitcoin" {
			continue
		}
		date := strings.SplitN(t.Timestamp, " ", 2)[0]
		iso, ok := currencyToISO[t.PaymentCurrency]
		if !ok {
			iso = t.PaymentCurrency
		}
		rate := 1.0
		found := false
		if dayRates, ok := rates[date]; ok {
			if r, ok2 := dayRates[iso]; ok2 {
				rate = r
				found = true
			}
		}
		if !found {
			continue
		}
		if t.AmountPaid/rate < _QUERY5_AMOUNT_THRESHOLD {
			count++
		}
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "Size\n%d\n", count)
	return err
}
