package normalizer

import "strings"

func NormalizeBankID(s string) string {
	t := strings.TrimLeft(s, "0")
	if t == "" {
		return "0"
	}
	return t
}
