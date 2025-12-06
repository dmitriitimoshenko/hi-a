package dto_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/magiconair/properties/assert"
)

func TestSalaryDataDTOToString(t *testing.T) {
	var nilDTO *dto.SalaryDataDTO

	assert.Equal(t, nilDTO.ToString(), "-")

	amountFrom := 1000.5
	amountTo := 2000.0
	dtoWithValues := &dto.SalaryDataDTO{
		AmountFrom: &amountFrom,
		AmountTo:   &amountTo,
		Currency:   "USD",
		Period:     enums.SalaryPeriodMonthly,
	}

	assert.Equal(
		t,
		dtoWithValues.ToString(),
		"amountFrom: 1000.5, amountTo: 2000, currency: USD, period: monthly",
	)

	dtoWithMissingAmount := &dto.SalaryDataDTO{
		Currency: "EUR",
		Period:   enums.SalaryPeriodYearly,
	}

	assert.Equal(
		t,
		dtoWithMissingAmount.ToString(),
		"amountFrom: -, amountTo: -, currency: EUR, period: yearly",
	)
}

func TestSalaryDataDTOIsEqual(t *testing.T) {
	var nilLeft *dto.SalaryDataDTO

	assert.Equal(t, nilLeft.IsEqual(nil), true)

	rightOnly := &dto.SalaryDataDTO{
		Currency: "USD",
		Period:   enums.SalaryPeriodMonthly,
	}

	assert.Equal(t, nilLeft.IsEqual(rightOnly), false)

	amountFrom := 1000.0
	amountTo := 2000.0

	left := &dto.SalaryDataDTO{
		AmountFrom: &amountFrom,
		AmountTo:   &amountTo,
		Currency:   "USD",
		Period:     enums.SalaryPeriodMonthly,
	}

	right := &dto.SalaryDataDTO{
		AmountFrom: &amountFrom,
		AmountTo:   &amountTo,
		Currency:   "USD",
		Period:     enums.SalaryPeriodMonthly,
	}

	assert.Equal(t, left.IsEqual(right), true)

	right.Currency = "EUR"
	assert.Equal(t, left.IsEqual(right), false)

	right.Currency = "USD"
	right.Period = enums.SalaryPeriodYearly
	assert.Equal(t, left.IsEqual(right), false)

	right.Period = enums.SalaryPeriodMonthly
	right.AmountFrom = nil
	assert.Equal(t, left.IsEqual(right), false)

	anotherAmount := 1500.0
	right.AmountFrom = &anotherAmount
	right.AmountTo = &amountTo
	assert.Equal(t, left.IsEqual(right), false)

	right.AmountFrom = &amountFrom
	right.AmountTo = nil
	assert.Equal(t, left.IsEqual(right), false)

	amountToDifferent := 2500.0
	right.AmountTo = &amountToDifferent
	assert.Equal(t, left.IsEqual(right), false)
}
