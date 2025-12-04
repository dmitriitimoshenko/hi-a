package tools

import (
	"regexp"
	"strconv"
	"strings"
)

var salaryPattern = regexp.MustCompile(`(?i)^\s*(\d+(?:[.,]\d+)?)(?:\s*-\s*(\d+(?:[.,]\d+)?))?\s*(k)?\s*$`)

// ParseSalaryRange converts human-readable salary text (e.g. "6-7k", "80k", "4000-6000")
// into numeric from/to values. Returns ok=false when the input cannot be parsed.
func ParseSalaryRange(text string) (from float64, to float64, ok bool) {
	matches := salaryPattern.FindStringSubmatch(strings.TrimSpace(text))
	if matches == nil {
		return 0, 0, false
	}

	scale := 1.0
	if matches[3] != "" {
		scale = 1000
	}

	parseNum := func(raw string) (float64, bool) {
		raw = strings.ReplaceAll(raw, ",", ".")
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, false
		}

		return val, true
	}

	left, ok := parseNum(matches[1])
	if !ok {
		return 0, 0, false
	}

	right := left
	if matches[2] != "" {
		right, ok = parseNum(matches[2])
		if !ok {
			return 0, 0, false
		}
	}

	return left * scale, right * scale, true
}
