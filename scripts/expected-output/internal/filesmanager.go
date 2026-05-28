package internal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Reading helpers

func readCSVRecords(path string) ([][]string, error) {
	fStat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fStat.Size() > 1*8*1024*1024*1024 {
		return nil, fmt.Errorf("Los archivos de prueba deben ser menores de 1GB. La ruta %s tiene un tamaño de %d bytes", path, fStat.Size())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var out [][]string
	sc.Scan() // skip header
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		out = append(out, strings.Split(line, ","))
	}
	return out, sc.Err()
}

func loadTransactions(inputDir string, n int) ([]Transaction, error) {
	path := filepath.Join(inputDir, fmt.Sprintf("transactions_%d.csv", n))
	rows, err := readCSVRecords(path)
	if err != nil {
		return nil, err
	}
	out := make([]Transaction, 0, len(rows))
	for _, r := range rows {
		if len(r) < 10 {
			continue
		}
		amount, _ := strconv.ParseFloat(r[7], 64)
		out = append(out, Transaction{
			Timestamp:         r[0],
			FromBank:          r[1],
			FromAccount:       r[2],
			ToBank:            r[3],
			ToAccount:         r[4],
			ReceivingCurrency: r[6],
			AmountPaid:        amount,
			PaymentCurrency:   r[8],
			PaymentFormat:     r[9],
		})
	}
	return out, nil
}

func loadAccounts(inputDir string, n int) ([]Account, error) {
	path := filepath.Join(inputDir, fmt.Sprintf("accounts_%d.csv", n))
	rows, err := readCSVRecords(path)
	if err != nil {
		return nil, err
	}
	out := make([]Account, 0, len(rows))
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		out = append(out, Account{BankName: r[0], BankID: r[1]})
	}
	return out, nil
}

// Writting helpers

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

func sortedLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
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
