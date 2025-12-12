package tools_test

import (
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
)

func TestParseSheetDateGivenInDaysSince(t *testing.T) {
	t.Parallel()
	t.Run("valid_value_integer", func(t *testing.T) {
		val, err := tools.ParseSheetDateGivenInDaysSince("50000")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val == nil {
			t.Fatalf("expected time value")
		}

		expected := time.Date(2036, time.November, 21, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, *val, expected)
	})

	t.Run("valid_value_fraction", func(t *testing.T) {
		val, err := tools.ParseSheetDateGivenInDaysSince("50000.5")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val == nil {
			t.Fatalf("expected time value")
		}

		expected := time.Date(2036, time.November, 21, 12, 0, 0, 0, time.UTC)
		assert.Equal(t, *val, expected)
	})

	t.Run("valid_value_one", func(t *testing.T) {
		val, err := tools.ParseSheetDateGivenInDaysSince("1")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val == nil {
			t.Fatalf("expected time value")
		}

		expected := time.Date(1899, time.December, 31, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, *val, expected)
	})

	t.Run("invalid_value", func(t *testing.T) {
		val, err := tools.ParseSheetDateGivenInDaysSince("invalid")

		assert.Equal(t, err == nil, false)
		assert.Equal(t, val == nil, true)
	})
}
