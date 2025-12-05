package dto

import (
	"fmt"
	"strconv"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
)

type SalaryDataDTO struct {
	AmountFrom *float64
	AmountTo   *float64
	Currency   string
	Period     enums.SalaryPeriod
}

func (dto *SalaryDataDTO) ToString() string {
	af := "-"
	if dto.AmountFrom != nil {
		af = strconv.FormatFloat(*dto.AmountFrom, 'f', -1, 64)
	}

	at := "-"
	if dto.AmountTo != nil {
		at = strconv.FormatFloat(*dto.AmountTo, 'f', -1, 64)
	}

	return fmt.Sprintf(
		"amountFrom: %s, amountTo: %s, currency: %s, period: %s",
		af, at, dto.Currency, dto.Period,
	)
}
