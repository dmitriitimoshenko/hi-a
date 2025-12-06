package dto

import (
	"strconv"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
)

type UpdateApplicationDTO struct {
	ID             int64
	RowID          int64
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
	Embedding      []float32
	SalaryApplied  *UpdateApplicationSalaryDTO
	SalaryProposed *UpdateApplicationSalaryDTO
}

func (dto *UpdateApplicationDTO) MapModel(dbApplication *models.Application) error {
	var stageIntPtr *int64
	if dbApplication.Stage != nil {
		stageInt, err := strconv.Atoi(*dbApplication.Stage)
		if err != nil {
			return err
		}
		stageIntPtr = tools.ToPtr(int64(stageInt))
	}

	var salaryApplied, salaryProposed *UpdateApplicationSalaryDTO
	if dbApplication.SalaryApplied != nil {
		salaryApplied = &UpdateApplicationSalaryDTO{
			AmountFrom: dbApplication.SalaryApplied.AmountFrom,
			AmountTo:   dbApplication.SalaryApplied.AmountTo,
			Currency:   dbApplication.SalaryApplied.Currency,
			Period:     dbApplication.SalaryApplied.Period,
		}
	}
	if dbApplication.SalaryProposed != nil {
		salaryProposed = &UpdateApplicationSalaryDTO{
			AmountFrom: dbApplication.SalaryProposed.AmountFrom,
			AmountTo:   dbApplication.SalaryProposed.AmountTo,
			Currency:   dbApplication.SalaryProposed.Currency,
			Period:     dbApplication.SalaryProposed.Period,
		}
	}

	dto.ID = dbApplication.ID
	dto.RowID = dbApplication.RowID
	dto.Company = dbApplication.Company
	dto.Title = dbApplication.Title
	dto.EmploymentType = dbApplication.EmploymentType
	dto.WorkMode = dbApplication.WorkMode
	dto.Status = dbApplication.Status
	dto.AppliedAt = dbApplication.AppliedAt
	dto.RespondedAt = dbApplication.RespondedAt
	dto.NextFollowUpAt = dbApplication.NextFollowUpAt
	dto.Stage = stageIntPtr
	dto.Meta = tools.FromJSONMap(dbApplication.Meta)
	dto.SalaryApplied = salaryApplied
	dto.SalaryProposed = salaryProposed

	return nil
}

type UpdateApplicationSalaryDTO struct {
	ID         int64
	AmountFrom *float64
	AmountTo   *float64
	Currency   string
	Period     enums.SalaryPeriod
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
}

type PaginationParams struct {
	Page     int64
	PageSize int64
}

type PaginatedApplications struct {
	CurrentPage  int64
	LastPage     int64
	NextPage     *int64
	PreviousPage *int64
	Content      []*models.Application
}
