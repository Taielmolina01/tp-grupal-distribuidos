package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ===== Config =====

var (
	inputDir    = envOr("INPUT_DIR", "./input")
	expectedDir = envOr("EXPECTED_DIR", "./expected_output")
	outputDir   = envOr("OUTPUT_DIR", "./output")
	nClients    = envInt("N_CLIENTS", 5)
)

const (
	usdCurrency       = "US Dollar"
	amountThresholdQ1 = 50.0
	avgFractionQ3     = 0.01
	minScatterQ4      = 5
	amountThresholdQ5 = 1.0
)

var validFormatsQ5 = map[string]bool{"Wire": true, "ACH": true}

type tx struct {
	Timestamp       string
	FromBank        string
	FromAccount     string
	ToBank          string
	ToAccount       string
	AmountPaid      float64
	PaymentCurrency string
	PaymentFormat   string
}

type account struct {
	BankName string
	BankID   string
}

type bankAcc struct {
	Bank    string
	Account string
}

var btcRatesUSD = map[string]float64{
	"2022-09-01": 19793.1,
	"2022-09-02": 199999.0,
	"2022-09-03": 19831.4,
	"2022-09-04": 19952.7,
	"2022-09-05": 20126.1,
}

var currencyToISO = map[string]string{
	"Australian Dollar": "AUD",
	"Brazil Real":       "BRL",
	"Canadian Dollar":   "CAD",
	"Swiss Franc":       "CHF",
	"Yuan":              "CNY",
	"Euro":              "EUR",
	"UK Pound":          "GBP",
	"Shekel":            "ILS",
	"Rupee":             "INR",
	"Yen":               "JPY",
	"Mexican Peso":      "MXN",
	"Ruble":             "RUB",
	"Saudi Riyal":       "SAR",
	"US Dollar":         "USD",
	"Bitcoin":           "BTC",
}

const frankfurterURL = "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD"

var rates map[string]map[string]float64

func fetchRates() (map[string]map[string]float64, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(frankfurterURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("frankfurter: HTTP %d", resp.StatusCode)
	}
	var entries []struct {
		Date  string  `json:"date"`
		Quote string  `json:"quote"`
		Rate  float64 `json:"rate"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	out := map[string]map[string]float64{}
	for _, e := range entries {
		date := strings.ReplaceAll(e.Date, "-", "/")
		if out[date] == nil {
			out[date] = map[string]float64{}
		}
		out[date][e.Quote] = e.Rate
	}
	for date := range out {
		out[date]["USD"] = 1.0
	}
	for d, r := range btcRatesUSD {
		date := strings.ReplaceAll(d, "-", "/")
		if out[date] == nil {
			out[date] = map[string]float64{}
		}
		out[date]["BTC"] = r
	}
	return out, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func normalizeBank(id string) string {
	s := strings.TrimLeft(id, "0")
	if s == "" {
		return "0"
	}
	return s
}

func fmtAmount(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func readCSVRecords(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	var out [][]string
	first := true
	for sc.Scan() {
		if first {
			first = false
			continue
		}
		line := sc.Text()
		if line == "" {
			continue
		}
		out = append(out, strings.Split(line, ","))
	}
	return out, sc.Err()
}

func loadTxs(n int) ([]tx, error) {
	path := filepath.Join(inputDir, fmt.Sprintf("transactions_%d.csv", n))
	rows, err := readCSVRecords(path)
	if err != nil {
		return nil, err
	}
	out := make([]tx, 0, len(rows))
	for _, r := range rows {
		if len(r) < 10 {
			continue
		}
		amount, _ := strconv.ParseFloat(r[7], 64)
		out = append(out, tx{
			Timestamp:       r[0],
			FromBank:        r[1],
			FromAccount:     r[2],
			ToBank:          r[3],
			ToAccount:       r[4],
			AmountPaid:      amount,
			PaymentCurrency: r[8],
			PaymentFormat:   r[9],
		})
	}
	return out, nil
}

func loadAccounts(n int) ([]account, error) {
	path := filepath.Join(inputDir, fmt.Sprintf("accounts_%d.csv", n))
	rows, err := readCSVRecords(path)
	if err != nil {
		return nil, err
	}
	out := make([]account, 0, len(rows))
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		out = append(out, account{BankName: r[0], BankID: r[1]})
	}
	return out, nil
}

// Queries

type csvOut struct {
	f *os.File
	w *bufio.Writer
}

func newCSVOut(path string) (*csvOut, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &csvOut{f: f, w: bufio.NewWriter(f)}, nil
}

func (c *csvOut) row(cells ...string) {
	c.w.WriteString(strings.Join(cells, ","))
	c.w.WriteByte('\n')
}

func (c *csvOut) close() error {
	if err := c.w.Flush(); err != nil {
		c.f.Close()
		return err
	}
	return c.f.Close()
}

func query1(txs []tx, outPath string) error {
	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "To Bank", "Account.1", "Amount Paid")
	for _, t := range txs {
		if t.PaymentCurrency == usdCurrency && t.AmountPaid < amountThresholdQ1 {
			out.row(t.FromBank, t.FromAccount, t.ToBank, t.ToAccount, fmtAmount(t.AmountPaid))
		}
	}
	return out.close()
}

func query2(txs []tx, accs []account, outPath string) error {
	type entry struct {
		amt float64
		t   tx
	}
	maxes := map[string]entry{}
	for _, t := range txs {
		if t.PaymentCurrency != usdCurrency {
			continue
		}
		cur, ok := maxes[normalizeBank(t.FromBank)]
		if !ok || t.AmountPaid > cur.amt {
			maxes[normalizeBank(t.FromBank)] = entry{t.AmountPaid, t}
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
	sort.Strings(keys)

	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "Bank Name", "Amount Paid")
	for _, k := range keys {
		m := maxes[k]
		name, ok := bankName[normalizeBank(k)]
		if !ok {
			continue
		}
		out.row(m.t.FromBank, m.t.FromAccount, name, fmtAmount(m.amt))
	}
	return out.close()
}

func query3(txs []tx, outPath string) error {
	sums := map[string]float64{}
	counts := map[string]int{}
	for _, t := range txs {
		if t.PaymentCurrency != usdCurrency {
			continue
		}
		if t.Timestamp >= "2022/09/01" && t.Timestamp <= "2022/09/06" {
			sums[t.PaymentFormat] += t.AmountPaid
			counts[t.PaymentFormat]++
		}
	}
	avg := map[string]float64{}
	for k, s := range sums {
		if counts[k] > 0 {
			avg[k] = s / float64(counts[k])
		}
	}

	out, err := newCSVOut(outPath)
	if err != nil {
		return err
	}
	out.row("From Bank", "Account", "Payment Format", "Amount Paid")
	for _, t := range txs {
		if t.PaymentCurrency != usdCurrency {
			continue
		}
		if !(t.Timestamp >= "2022/09/06" && t.Timestamp < "2022/09/16") {
			continue
		}
		a, ok := avg[t.PaymentFormat]
		if !ok {
			continue
		}
		if t.AmountPaid < a*avgFractionQ3 {
			out.row(t.FromBank, t.FromAccount, t.PaymentFormat, fmtAmount(t.AmountPaid))
		}
	}
	return out.close()
}

func query4(txs []tx, outPath string) error {
	var p1 []tx
	for _, t := range txs {
		if t.PaymentCurrency == usdCurrency && t.Timestamp >= "2022/09/01" && t.Timestamp <= "2022/09/06" {
			p1 = append(p1, t)
		}
	}

	distinct := map[bankAcc]map[bankAcc]bool{}
	for _, t := range p1 {
		src := bankAcc{t.FromBank, t.FromAccount}
		dst := bankAcc{t.ToBank, t.ToAccount}
		if distinct[src] == nil {
			distinct[src] = map[bankAcc]bool{}
		}
		distinct[src][dst] = true
	}
	scatterSrcs := map[bankAcc]bool{}
	for src, dsts := range distinct {
		if len(dsts) > minScatterQ4 {
			scatterSrcs[src] = true
		}
	}

	type edge struct{ Src, Dst bankAcc }
	var edges []edge
	for _, t := range p1 {
		src := bankAcc{t.FromBank, t.FromAccount}
		if scatterSrcs[src] {
			edges = append(edges, edge{src, bankAcc{t.ToBank, t.ToAccount}})
		}
	}

	countSM := map[bankAcc]map[bankAcc]int{}
	for _, e := range edges {
		if countSM[e.Src] == nil {
			countSM[e.Src] = map[bankAcc]int{}
		}
		countSM[e.Src][e.Dst]++
	}

	type pair struct{ A, C bankAcc }
	paths := map[pair]int{}
	for a, midMap := range countSM {
		for mid, ab := range midMap {
			for c, bc := range countSM[mid] {
				if a == c {
					continue
				}
				paths[pair{a, c}] += ab * bc
			}
		}
	}

	unique := map[bankAcc]bool{}
	for p, n := range paths {
		if n > minScatterQ4 {
			unique[p.A] = true
			unique[p.C] = true
		}
	}

	accs := make([]bankAcc, 0, len(unique))
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

func query5(txs []tx, outPath string) error {
	count := 0
	for _, t := range txs {
		if !(t.Timestamp >= "2022/09/01" && t.Timestamp <= "2022/09/06") {
			continue
		}
		if !validFormatsQ5[t.PaymentFormat] {
			continue
		}
		date := strings.SplitN(t.Timestamp, " ", 2)[0]
		iso, ok := currencyToISO[t.PaymentCurrency]
		if !ok {
			iso = t.PaymentCurrency
		}
		rate := 1.0
		if dayRates, ok := rates[date]; ok {
			if r, ok2 := dayRates[iso]; ok2 {
				rate = r
			}
		}
		if t.AmountPaid/rate < amountThresholdQ5 {
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

func hashDataset(n int) (string, error) {
	h := md5.New()
	for _, name := range []string{
		fmt.Sprintf("transactions_%d.csv", n),
		fmt.Sprintf("accounts_%d.csv", n),
	} {
		f, err := os.Open(filepath.Join(inputDir, name))
		if err != nil {
			return "", err
		}
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func build() error {
	if err := os.MkdirAll(expectedDir, 0o755); err != nil {
		return err
	}

	log.Printf("Fetching rates from %s", frankfurterURL)
	r, err := fetchRates()
	if err != nil {
		return fmt.Errorf("no se pudieron obtener las cotizaciones: %w", err)
	}
	rates = r

	hashToRep := map[string]int{}
	mapping := map[int]int{}
	for n := 0; n < nClients; n++ {
		h, err := hashDataset(n)
		if err != nil {
			return err
		}
		if _, ok := hashToRep[h]; !ok {
			hashToRep[h] = n
		}
		mapping[n] = hashToRep[h]
	}

	seen := map[int]bool{}
	var reps []int
	for _, r := range mapping {
		if !seen[r] {
			seen[r] = true
			reps = append(reps, r)
		}
	}
	sort.Ints(reps)

	for _, rep := range reps {
		log.Printf("Procesando dataset (rep cliente %d)", rep)
		txs, err := loadTxs(rep)
		if err != nil {
			return err
		}
		accs, err := loadAccounts(rep)
		if err != nil {
			return err
		}
		prefix := filepath.Join(expectedDir, fmt.Sprintf("expected_%d", rep))
		if err := query1(txs, prefix+"_1.csv"); err != nil {
			return err
		}
		log.Print("  q1 listo")
		if err := query2(txs, accs, prefix+"_2.csv"); err != nil {
			return err
		}
		log.Print("  q2 listo")
		if err := query3(txs, prefix+"_3.csv"); err != nil {
			return err
		}
		log.Print("  q3 listo")
		if err := query4(txs, prefix+"_4.csv"); err != nil {
			return err
		}
		log.Print("  q4 listo")
		if err := query5(txs, prefix+"_5.csv"); err != nil {
			return err
		}
		log.Print("  q5 listo")
	}

	mPath := filepath.Join(expectedDir, "manifest.txt")
	mf, err := os.Create(mPath)
	if err != nil {
		return err
	}
	defer mf.Close()
	keys := make([]int, 0, len(mapping))
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Fprintf(mf, "%d %d\n", k, mapping[k])
	}
	log.Printf("Manifest: %s", mPath)
	return nil
}

func sortedLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Strings(lines)
	return lines, nil
}

func diffSorted(a, b []string) []string {
	var out []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case a[i] < b[j]:
			out = append(out, "< "+a[i])
			i++
		default:
			out = append(out, "> "+b[j])
			j++
		}
	}
	for i < len(a) {
		out = append(out, "< "+a[i])
		i++
	}
	for j < len(b) {
		out = append(out, "> "+b[j])
		j++
	}
	return out
}

type pairCR struct{ client, rep int }

func readManifest() ([]pairCR, error) {
	mPath := filepath.Join(expectedDir, "manifest.txt")
	f, err := os.Open(mPath)
	if err != nil {
		return nil, fmt.Errorf("falta %s, corre 'build' primero: %w", mPath, err)
	}
	defer f.Close()
	var out []pairCR
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var c, r int
		if _, err := fmt.Sscanf(sc.Text(), "%d %d", &c, &r); err != nil {
			continue
		}
		out = append(out, pairCR{c, r})
	}
	return out, sc.Err()
}

func verify() error {
	pairs, err := readManifest()
	if err != nil {
		return err
	}

	allOk := true
	for _, p := range pairs {
		for q := 1; q <= 5; q++ {
			tag := fmt.Sprintf("[client %d q%d]", p.client, q)
			outPath := filepath.Join(outputDir, fmt.Sprintf("output_%d_%d.csv", p.client, q))
			expPath := filepath.Join(expectedDir, fmt.Sprintf("expected_%d_%d.csv", p.rep, q))
			if _, err := os.Stat(outPath); err != nil {
				fmt.Println(tag, "MISSING output", outPath)
				allOk = false
				continue
			}
			if _, err := os.Stat(expPath); err != nil {
				fmt.Println(tag, "MISSING expected", expPath)
				allOk = false
				continue
			}
			outLines, err := sortedLines(outPath)
			if err != nil {
				return err
			}
			expLines, err := sortedLines(expPath)
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

func main() {
	log.SetFlags(log.LstdFlags)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: expected-output build | verify")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "build":
		err = build()
	case "verify":
		err = verify()
	default:
		fmt.Fprintf(os.Stderr, "subcomando desconocido: %s (usa 'build' o 'verify')\n", os.Args[1])
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}
