package services

import (
	"context"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type kafkaPublisher interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}

type applicationRepository interface {
	FindByID(ctx context.Context, id int64) (*models.Application, error)
	FindByRowID(ctx context.Context, rowID int64) (*models.Application, error)
	Save(ctx context.Context, application ...*models.Application) error
	GetMaxRowID(ctx context.Context) (*int64, error)
	List(
		ctx context.Context,
		applicationStatusInclude []enums.ApplicationStatus,
		ApplicationStatusExclude []enums.ApplicationStatus,
		IsReplyEmailReceived bool,
	) ([]*models.Application, error)
	Paginate(ctx context.Context, pp dto.PaginationParams) (*dto.PaginatedApplications, error)
}

type salaryService interface {
	Save(ctx context.Context, salary ...*models.Salary) error
	FindByID(ctx context.Context, id int64) (*models.Salary, error)
}

type salaryRepository interface {
	Save(ctx context.Context, salary ...*models.Salary) error
	FindByID(ctx context.Context, id int64) (*models.Salary, error)
}

type sheetsClient interface {
	Read(ctx context.Context, sheetRange string) ([][]string, error)
	Write(ctx context.Context, value string, ceil string) error
}

type sheetsService interface {
	GetApplicationFromRow(ctx context.Context, rowID int64) (*dto.SheetApplicationDTO, error)
	GetApplicationsFromRows(ctx context.Context, rowFrom int64, rowTo int64) (map[int64]dto.SheetApplicationDTO, error)

	SetCompany(ctx context.Context, rowID int64, company string) error
	SetEmploymentType(ctx context.Context, rowID int64, employmentType enums.EmploymentType) error
	SetWorkMode(ctx context.Context, rowID int64, workMode enums.WorkMode) error
	SetTitle(ctx context.Context, rowID int64, title string) error
	SetSalaryApplied(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error
	SetSalaryProposed(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error
	SetStatus(ctx context.Context, rowID int64, status enums.ApplicationStatus) error
	SetAppliedAt(ctx context.Context, rowID int64, appliedAt time.Time) error
	SetRespondedAt(ctx context.Context, rowID int64, respondedAt *time.Time) error
	SetNextFollowUpAt(ctx context.Context, rowID int64, nextFollowUpAt *time.Time) error
	SetStage(ctx context.Context, rowID int64, stage *int64) error
	SetContacts(ctx context.Context, rowID int64, contacts string) error
	SetJobDescription(ctx context.Context, rowID int64, jobDescription string) error
	SetNotes(ctx context.Context, rowID int64, notes string) error
}
