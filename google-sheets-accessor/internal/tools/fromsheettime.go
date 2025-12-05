package tools

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

const day = 24 * time.Hour

func ParseSheetDateGivenInDaysSince(l *slog.Logger, value string) (*time.Time, error) {
	days, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sheet date value [%s]: %w", value, err)
	}

	l.Info(
		"ParseSheetDateGivenInDaysSince parsing",
		slog.Float64("days", days),
	)

	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)

	r := base.Add(time.Duration(days * float64(day)))

	return &r, nil
}
