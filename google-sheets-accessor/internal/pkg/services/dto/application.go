package dto

import (
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
)

type UpdateApplicationDTO struct {
	ID             int64
	RowID          int64
	Company        string
	Title          string
	EmploymentType string
	WorkMode       string
	Status         string
	AppliedAt      time.Time
	RespondedAt    *time.Time
	NextFollowUpAt *time.Time
	Stage          *int64
	Meta           map[string]string
	Embedding      []float32
	SalaryApplied  *UpdateApplicationSalaryDTO
	SalaryProposed *UpdateApplicationSalaryDTO
}

type UpdateApplicationSalaryDTO struct {
	ID         int64
	AmountFrom *float64
	AmountTo   *float64
	Currency   string
	Period     string
}

type SheetApplicationDTO struct {
	Company        string
	Title          string
	EmploymentType enums.EmploymentType
	WorkMode       enums.WorkMode
	Status         enums.ApplicationStatus
	AppliedAt      time.Time
	RespondedAt    *time.Time
	NextFollowUpAt *time.Time
	Stage          *int64
	Meta           map[string]string
	SalaryApplied  *SheetApplicationSalaryDTO
	SalaryProposed *SheetApplicationSalaryDTO
}

type SheetApplicationSalaryDTO struct {
	AmountFrom *float64
	AmountTo   *float64
	Currency   string
	Period     enums.SalaryPeriod
}

type ApplicationDiffEntry struct {
	RowID       int64             `json:"row_id"`
	Company     *string           `json:"company,omitempty"`
	RoleTitle   *string           `json:"role_title,omitempty"`
	Differences []ApplicationDiff `json:"differences"`
	Errors      []string          `json:"errors"`
}

type ApplicationDiff struct {
	Field      string
	SheetValue interface{}
	DBValue    interface{}
	Message    string
}
