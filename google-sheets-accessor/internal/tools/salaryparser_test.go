package tools_test

import (
	"strings"
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
)

func TestParseSalaryRange(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [2]float64
		ok       bool
	}{
		{name: "single_k", input: "80k", expected: [2]float64{80000, 80000}, ok: true},
		{name: "range_k", input: "6-7k", expected: [2]float64{6000, 7000}, ok: true},
		{name: "single_plain", input: "3000000", expected: [2]float64{3000000, 3000000}, ok: true},
		{name: "range_plain", input: "4000-6000", expected: [2]float64{4000, 6000}, ok: true},
		{name: "range_mixed_k", input: "60-100k", expected: [2]float64{60000, 100000}, ok: true},
		{name: "single_small_k", input: "2k", expected: [2]float64{2000, 2000}, ok: true},
		{name: "invalid_text", input: "abc", ok: false},
		{name: "overflow_single", input: strings.Repeat("9", 4000), ok: false},
		{name: "overflow_range_right", input: "1-" + strings.Repeat("9", 4000), ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, ok := tools.ParseSalaryRange(tt.input)
			assert.Equal(t, ok, tt.ok)
			if tt.ok {
				assert.Equal(t, from, tt.expected[0])
				assert.Equal(t, to, tt.expected[1])
			}
		})
	}
}
