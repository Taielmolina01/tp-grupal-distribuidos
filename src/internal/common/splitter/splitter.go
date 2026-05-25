package splitter

import "strings"

func Split(str string, separator string) []string {
	if str == "" {
		return nil
	}
	parts := strings.Split(str, separator)
	output := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			output = append(output, p)
		}
	}
	return output
}
