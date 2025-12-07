package mocks

import (
	"context"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/stretchr/testify/mock"
)

type ApplicationRepositoryMock struct {
	mock.Mock
}

func (m *ApplicationRepositoryMock) FindByID(ctx context.Context, id int64) (*models.Application, error) {
	args := m.Called(ctx, id)

	app, _ := args.Get(0).(*models.Application)

	return app, args.Error(1)
}

func (m *ApplicationRepositoryMock) FindByRowID(ctx context.Context, rowID int64) (*models.Application, error) {
	args := m.Called(ctx, rowID)

	app, _ := args.Get(0).(*models.Application)

	return app, args.Error(1)
}

func (m *ApplicationRepositoryMock) Save(ctx context.Context, application ...*models.Application) error {
	args := m.Called(ctx, application)

	return args.Error(0)
}

func (m *ApplicationRepositoryMock) GetMaxRowID(ctx context.Context) (*int64, error) {
	args := m.Called(ctx)

	rowID, _ := args.Get(0).(*int64)

	return rowID, args.Error(1)
}

func (m *ApplicationRepositoryMock) List(
	ctx context.Context,
	applicationStatusInclude []enums.ApplicationStatus,
	applicationStatusExclude []enums.ApplicationStatus,
	isReplyEmailReceived bool,
) ([]*models.Application, error) {
	args := m.Called(ctx, applicationStatusInclude, applicationStatusExclude, isReplyEmailReceived)

	apps, _ := args.Get(0).([]*models.Application)

	return apps, args.Error(1)
}

func (m *ApplicationRepositoryMock) Paginate(ctx context.Context, pp dto.PaginationParams) (*dto.PaginatedApplications, error) {
	args := m.Called(ctx, pp)

	result, _ := args.Get(0).(*dto.PaginatedApplications)

	return result, args.Error(1)
}

type KafkaPublisherMock struct {
	mock.Mock
}

func (m *KafkaPublisherMock) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	args := m.Called(ctx, topic, key, value)

	return args.Error(0)
}

type SheetsServiceMock struct {
	mock.Mock
}

func (m *SheetsServiceMock) GetApplicationFromRow(ctx context.Context, rowID int64) (*dto.SheetApplicationDTO, error) {
	args := m.Called(ctx, rowID)

	app, _ := args.Get(0).(*dto.SheetApplicationDTO)

	return app, args.Error(1)
}

func (m *SheetsServiceMock) GetApplicationsFromRows(ctx context.Context, rowFrom int64, rowTo int64) (map[int64]dto.SheetApplicationDTO, error) {
	args := m.Called(ctx, rowFrom, rowTo)

	apps, _ := args.Get(0).(map[int64]dto.SheetApplicationDTO)

	return apps, args.Error(1)
}

func (m *SheetsServiceMock) SetCompany(ctx context.Context, rowID int64, company string) error {
	args := m.Called(ctx, rowID, company)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetEmploymentType(ctx context.Context, rowID int64, employmentType enums.EmploymentType) error {
	args := m.Called(ctx, rowID, employmentType)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetWorkMode(ctx context.Context, rowID int64, workMode enums.WorkMode) error {
	args := m.Called(ctx, rowID, workMode)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetTitle(ctx context.Context, rowID int64, title string) error {
	args := m.Called(ctx, rowID, title)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetSalaryApplied(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error {
	args := m.Called(ctx, rowID, salary)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetSalaryProposed(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error {
	args := m.Called(ctx, rowID, salary)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetStatus(ctx context.Context, rowID int64, status enums.ApplicationStatus) error {
	args := m.Called(ctx, rowID, status)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetAppliedAt(ctx context.Context, rowID int64, appliedAt time.Time) error {
	args := m.Called(ctx, rowID, appliedAt)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetRespondedAt(ctx context.Context, rowID int64, respondedAt *time.Time) error {
	args := m.Called(ctx, rowID, respondedAt)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetNextFollowUpAt(ctx context.Context, rowID int64, nextFollowUpAt *time.Time) error {
	args := m.Called(ctx, rowID, nextFollowUpAt)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetStage(ctx context.Context, rowID int64, stage *int64) error {
	args := m.Called(ctx, rowID, stage)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetContacts(ctx context.Context, rowID int64, contacts string) error {
	args := m.Called(ctx, rowID, contacts)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetJobDescription(ctx context.Context, rowID int64, jobDescription string) error {
	args := m.Called(ctx, rowID, jobDescription)

	return args.Error(0)
}

func (m *SheetsServiceMock) SetNotes(ctx context.Context, rowID int64, notes string) error {
	args := m.Called(ctx, rowID, notes)

	return args.Error(0)
}

type SalaryServiceMock struct {
	mock.Mock
}

func (m *SalaryServiceMock) Save(ctx context.Context, salary ...*models.Salary) error {
	args := m.Called(ctx, salary)

	return args.Error(0)
}

func (m *SalaryServiceMock) FindByID(ctx context.Context, id int64) (*models.Salary, error) {
	args := m.Called(ctx, id)

	salary, _ := args.Get(0).(*models.Salary)

	return salary, args.Error(1)
}
