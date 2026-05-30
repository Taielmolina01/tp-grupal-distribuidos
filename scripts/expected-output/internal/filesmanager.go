package internal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const _SCANNER_MAX_LINE = 1024 * 1024

// Reading helpers

func forEachTransaction(inputDir string, n int, fn func(Transaction) error) error {
	path := filepath.Join(inputDir, fmt.Sprintf("transactions_%d.csv", n))
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), _SCANNER_MAX_LINE)
	sc.Scan()
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		r := strings.Split(line, ",")
		if len(r) < 10 {
			continue
		}
		amount, _ := strconv.ParseFloat(r[7], 64)
		if err := fn(Transaction{
			Timestamp:         r[0],
			FromBank:          r[1],
			FromAccount:       r[2],
			ToBank:            r[3],
			ToAccount:         r[4],
			ReceivingCurrency: r[6],
			AmountPaid:        amount,
			PaymentCurrency:   r[8],
			PaymentFormat:     r[9],
		}); err != nil {
			return err
		}
	}
	return sc.Err()
}

func loadAccounts(inputDir string, n int) ([]Account, error) {
	path := filepath.Join(inputDir, fmt.Sprintf("accounts_%d.csv", n))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), _SCANNER_MAX_LINE)
	sc.Scan()
	var out []Account
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		r := strings.Split(line, ",")
		if len(r) < 2 {
			continue
		}
		out = append(out, Account{BankName: r[0], BankID: r[1]})
	}
	return out, sc.Err()
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


type sortedStream struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	sc     *bufio.Scanner
}

func openSortedStream(path string) (*sortedStream, error) {
	cmd := exec.Command("sort", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64*1024), _SCANNER_MAX_LINE)
	return &sortedStream{cmd: cmd, stdout: stdout, sc: sc}, nil
}

func (s *sortedStream) close() {
	io.Copy(io.Discard, s.stdout)
	s.cmd.Wait()
}

func diffSortedFiles(tag, aPath, bPath string, w io.Writer) (bool, error) {
	a, err := openSortedStream(aPath)
	if err != nil {
		return false, err
	}
	defer a.close()
	b, err := openSortedStream(bPath)
	if err != nil {
		return false, err
	}
	defer b.close()

	var lineA, lineB string
	hasA := a.sc.Scan()
	if hasA {
		lineA = a.sc.Text()
	}
	hasB := b.sc.Scan()
	if hasB {
		lineB = b.sc.Text()
	}
	diffs := false
	for hasA && hasB {
		switch {
		case lineA == lineB:
			hasA = a.sc.Scan()
			if hasA {
				lineA = a.sc.Text()
			}
			hasB = b.sc.Scan()
			if hasB {
				lineB = b.sc.Text()
			}
		case lineA < lineB:
			fmt.Fprintln(w, tag, "<", lineA)
			diffs = true
			hasA = a.sc.Scan()
			if hasA {
				lineA = a.sc.Text()
			}
		default:
			fmt.Fprintln(w, tag, ">", lineB)
			diffs = true
			hasB = b.sc.Scan()
			if hasB {
				lineB = b.sc.Text()
			}
		}
	}
	for hasA {
		fmt.Fprintln(w, tag, "<", lineA)
		diffs = true
		hasA = a.sc.Scan()
		if hasA {
			lineA = a.sc.Text()
		}
	}
	for hasB {
		fmt.Fprintln(w, tag, ">", lineB)
		diffs = true
		hasB = b.sc.Scan()
		if hasB {
			lineB = b.sc.Text()
		}
	}
	if err := a.sc.Err(); err != nil {
		return diffs, err
	}
	if err := b.sc.Err(); err != nil {
		return diffs, err
	}
	return diffs, nil
}
