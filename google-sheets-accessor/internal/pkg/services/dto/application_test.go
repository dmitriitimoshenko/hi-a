package dto_test

import (
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/magiconair/properties/assert"
	"gorm.io/datatypes"
)

func TestUpdateApplicationDTOMapModelSuccess(t *testing.T) {
	stage := "5"
	meta := datatypes.JSONMap{
		"foo": "bar",
		"num": 10,
	}

	appliedAt := time.Date(2024, time.May, 1, 12, 0, 0, 0, time.UTC)
	respondedAt := appliedAt.Add(24 * time.Hour)
	nextFollowUpAt := appliedAt.Add(48 * time.Hour)

	salaryAppliedAmountFrom := 1000.0
	salaryAppliedAmountTo := 2000.0
	salaryProposedAmountFrom := 3000.0
	salaryProposedAmountTo := 4000.0

	dbApplication := &models.Application{
		ID:             1,
		RowID:          2,
		Company:        "Acme",
		Title:          "Engineer",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeHybrid,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      appliedAt,
		RespondedAt:    &respondedAt,
		NextFollowUpAt: &nextFollowUpAt,
		Stage:          &stage,
		Meta:           &meta,
		SalaryApplied: &models.Salary{
			AmountFrom: &salaryAppliedAmountFrom,
			AmountTo:   &salaryAppliedAmountTo,
			Currency:   "USD",
			Period:     enums.SalaryPeriodMonthly,
		},
		SalaryProposed: &models.Salary{
			AmountFrom: &salaryProposedAmountFrom,
			AmountTo:   &salaryProposedAmountTo,
			Currency:   "EUR",
			Period:     enums.SalaryPeriodYearly,
		},
	}

	var applicationDTO dto.UpdateApplicationDTO

	err := applicationDTO.MapModel(dbApplication)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	assert.Equal(t, applicationDTO.ID, dbApplication.ID)
	assert.Equal(t, applicationDTO.RowID, dbApplication.RowID)
	assert.Equal(t, applicationDTO.Company, dbApplication.Company)
	assert.Equal(t, applicationDTO.Title, dbApplication.Title)
	assert.Equal(t, applicationDTO.EmploymentType, dbApplication.EmploymentType)
	assert.Equal(t, applicationDTO.WorkMode, dbApplication.WorkMode)
	assert.Equal(t, applicationDTO.Status, dbApplication.Status)
	assert.Equal(t, applicationDTO.AppliedAt, dbApplication.AppliedAt)
	assert.Equal(t, applicationDTO.RespondedAt, dbApplication.RespondedAt)
	assert.Equal(t, applicationDTO.NextFollowUpAt, dbApplication.NextFollowUpAt)

	if applicationDTO.Stage == nil {
		t.Fatalf("expected stage to be set")
	}

	assert.Equal(t, *applicationDTO.Stage, int64(5))
	assert.Equal(t, applicationDTO.Meta["foo"], "bar")
	assert.Equal(t, applicationDTO.Meta["num"], "10")

	if applicationDTO.SalaryApplied == nil || applicationDTO.SalaryProposed == nil {
		t.Fatalf("expected salary pointers to be set")
	}

	assert.Equal(t, applicationDTO.SalaryApplied.AmountFrom, dbApplication.SalaryApplied.AmountFrom)
	assert.Equal(t, applicationDTO.SalaryApplied.AmountTo, dbApplication.SalaryApplied.AmountTo)
	assert.Equal(t, applicationDTO.SalaryApplied.Currency, dbApplication.SalaryApplied.Currency)
	assert.Equal(t, applicationDTO.SalaryApplied.Period, dbApplication.SalaryApplied.Period)

	assert.Equal(t, applicationDTO.SalaryProposed.AmountFrom, dbApplication.SalaryProposed.AmountFrom)
	assert.Equal(t, applicationDTO.SalaryProposed.AmountTo, dbApplication.SalaryProposed.AmountTo)
	assert.Equal(t, applicationDTO.SalaryProposed.Currency, dbApplication.SalaryProposed.Currency)
	assert.Equal(t, applicationDTO.SalaryProposed.Period, dbApplication.SalaryProposed.Period)
}

func TestUpdateApplicationDTOMapModelNilStage(t *testing.T) {
	dbApplication := &models.Application{
		ID:             10,
		RowID:          20,
		Company:        "Acme",
		Title:          "Engineer",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeHybrid,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      time.Now(),
	}

	var applicationDTO dto.UpdateApplicationDTO

	err := applicationDTO.MapModel(dbApplication)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if applicationDTO.Stage != nil {
		t.Fatalf("expected stage to stay nil")
	}
}

func TestUpdateApplicationDTOMapModelInvalidStage(t *testing.T) {
	invalidStage := "not-an-int"

	dbApplication := &models.Application{
		Stage: &invalidStage,
	}

	var applicationDTO dto.UpdateApplicationDTO

	err := applicationDTO.MapModel(dbApplication)
	if err == nil {
		t.Fatalf("expected an error for invalid stage")
	}
}
