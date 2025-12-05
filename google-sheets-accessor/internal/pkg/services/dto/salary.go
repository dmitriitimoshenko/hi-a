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

func (dto *SalaryDataDTO) IsEqual(newDTO *SalaryDataDTO) bool {
	if dto == nil && newDTO == nil {
		return true
	}

	if (dto != nil && newDTO == nil) || (dto == nil && newDTO != nil) {
		return false
	}

	if dto.Currency != newDTO.Currency ||
		dto.Period != newDTO.Period {
		return false
	}

	if (dto.AmountFrom != nil && newDTO.AmountFrom == nil) ||
		(dto.AmountFrom == nil && newDTO.AmountFrom != nil) ||
		(dto.AmountTo != nil && newDTO.AmountTo == nil) ||
		(dto.AmountTo == nil && newDTO.AmountTo != nil) {
		return false
	}

	if dto.AmountFrom != nil && newDTO.AmountFrom != nil &&
		*dto.AmountFrom != *newDTO.AmountFrom {
		return false
	}

	if dto.AmountTo != nil && newDTO.AmountTo != nil &&
		*dto.AmountTo != *newDTO.AmountTo {
		return false
	}

	return true
}
