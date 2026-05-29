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

func getQuery1Result(inputDir string, n int, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "To Bank", "Account.1", "Amount Paid")
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.PaymentCurrency == _USD_CURRENCY && t.AmountPaid < _QUERY1_AMOUNT_THRESHOLD {
			out.row(t.FromBank, t.FromAccount, t.ToBank, t.ToAccount, fmtAmount(t.AmountPaid))
		}
		return nil
	}); err != nil {
		out.close()
		return err
	}
	return out.close()
}

func getQuery2Result(inputDir string, n int, accs []Account, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}

	maxes := map[string]Transaction{}
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.PaymentCurrency != _USD_CURRENCY {
			return nil
		}
		cur, ok := maxes[normalizeBank(t.FromBank)]
		if !ok || t.AmountPaid > cur.AmountPaid {
			maxes[normalizeBank(t.FromBank)] = t
		}
		return nil
	}); err != nil {
		out.close()
		return err
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

func getQuery3Result(inputDir string, n int, outPath string) error {
	type entry struct {
		AmountPaid float64
		Total      int32
	}
	sumAndCounts := map[string]entry{}
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.PaymentCurrency != _USD_CURRENCY {
			return nil
		}
		if t.Timestamp >= _QUERY3_FIRST_RANGE_START_DATE && t.Timestamp <= _QUERY3_FIRST_RANGE_END_DATE {
			oldValue := sumAndCounts[t.PaymentFormat]
			sumAndCounts[t.PaymentFormat] = entry{
				AmountPaid: oldValue.AmountPaid + t.AmountPaid,
				Total:      oldValue.Total + 1,
			}
		}
		return nil
	}); err != nil {
		return err
	}
	avg := map[string]float64{}
	for k, s := range sumAndCounts {
		if s.Total > 0 {
			avg[k] = s.AmountPaid / float64(s.Total)
		}
	}

	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "Payment Format", "Amount Paid")
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.PaymentCurrency != _USD_CURRENCY ||
			(t.Timestamp < _QUERY3_SECOND_RANGE_START_DATE || t.Timestamp >= _QUERY3_SECOND_RANGE_END_DATE) {
			return nil
		}
		a, ok := avg[t.PaymentFormat]
		if !ok {
			return nil
		}
		if t.AmountPaid < a*_QUERY3_AVG_FRACTION {
			out.row(t.FromBank, t.FromAccount, t.PaymentFormat, fmtAmount(t.AmountPaid))
		}
		return nil
	}); err != nil {
		out.close()
		return err
	}
	return out.close()
}

func getQuery4Result(inputDir string, n int, outPath string) error {
	scatterMap := map[BankAcc]map[BankAcc]bool{}
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.PaymentCurrency != _USD_CURRENCY ||
			t.Timestamp < _QUERY4_START_DATE || t.Timestamp >= _QUERY4_END_DATE {
			return nil
		}
		a := BankAcc{t.FromBank, t.FromAccount}
		b := BankAcc{t.ToBank, t.ToAccount}
		dests := scatterMap[a]
		if dests == nil {
			dests = map[BankAcc]bool{}
			scatterMap[a] = dests
		}
		dests[b] = true
		return nil
	}); err != nil {
		return err
	}

	reverseMap := map[BankAcc]map[BankAcc]bool{}
	for src, dests := range scatterMap {
		if len(dests) < _QUERY4_MIN_SCATTER {
			continue
		}
		for mid := range dests {
			sources := reverseMap[mid]
			if sources == nil {
				sources = map[BankAcc]bool{}
				reverseMap[mid] = sources
			}
			sources[src] = true
		}
	}
	scatterMap = nil

	type pair struct{ S, D BankAcc }
	chainMiddles := map[pair]map[BankAcc]bool{}

	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if t.Timestamp < _QUERY4_START_DATE || t.Timestamp >= _QUERY4_END_DATE {
			return nil
		}
		a := BankAcc{t.FromBank, t.FromAccount}
		sources, ok := reverseMap[a]
		if !ok {
			return nil
		}
		b := BankAcc{t.ToBank, t.ToAccount}
		for src := range sources {
			p := pair{src, b}
			mids := chainMiddles[p]
			if mids == nil {
				mids = map[BankAcc]bool{}
				chainMiddles[p] = mids
			}
			mids[a] = true
		}
		return nil
	}); err != nil {
		return err
	}

	unique := map[BankAcc]bool{}
	for p, mids := range chainMiddles {
		if len(mids) >= _QUERY4_MIN_SCATTER {
			unique[p.S] = true
			unique[p.D] = true
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

func getQuery5Result(inputDir string, n int, rates map[string]map[string]float64, outPath string) error {
	count := 0
	if err := forEachTransaction(inputDir, n, func(t Transaction) error {
		if !(t.Timestamp >= _QUERY5_START_DATE && t.Timestamp < _QUERY5_END_DATE) ||
			!slices.Contains(validFormatsQ5, t.PaymentFormat) ||
			t.ReceivingCurrency == "Bitcoin" {
			return nil
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
			return nil
		}
		if t.AmountPaid/rate < _QUERY5_AMOUNT_THRESHOLD {
			count++
		}
		return nil
	}); err != nil {
		return err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "Size\n%d\n", count)
	return err
}
