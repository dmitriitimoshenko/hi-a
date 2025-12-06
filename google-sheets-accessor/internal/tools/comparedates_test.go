package tools_test

import (
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
)

func TestAreDatesEqual(t *testing.T) {
	a := time.Date(2024, time.May, 10, 12, 0, 0, 0, time.UTC)
	b := time.Date(2024, time.May, 10, 18, 30, 0, 0, time.FixedZone("UTC+1", 3600))
	c := time.Date(2024, time.May, 11, 1, 0, 0, 0, time.UTC)

	assert.Equal(t, tools.AreDatesEqual(a, b), true)
	assert.Equal(t, tools.AreDatesEqual(a, c), false)
}
