package tools

import (
	"fmt"
	"strconv"
	"time"
)

const day = 24 * time.Hour

func ParseSheetDateGivenInDaysSince(value, layout string) (*time.Time, error) {
	days, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sheet date value [%s]: %w", value, err)
	}

	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)

	r := base.Add(time.Duration(days) * day)

	return &r, nil
}
