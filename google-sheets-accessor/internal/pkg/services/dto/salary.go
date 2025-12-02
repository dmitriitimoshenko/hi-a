package dto

import "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"

type SalaryDataDTO struct {
	AmountFrom *float64
	AmountTo   *float64
	Currency   string
	Period     enums.SalaryPeriod
}
