package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const frankfurterURL = "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD"

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
	return out, nil
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
