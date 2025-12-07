package services_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/mocks"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools/testhelper/mockdb"
	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

func newApplicationService(
	db *gorm.DB,
	repo *mocks.ApplicationRepositoryMock,
	sheetsSvc *mocks.SheetsServiceMock,
	kafka *mocks.KafkaPublisherMock,
	salary *mocks.SalaryServiceMock,
) *services.ApplicationService {
	return services.NewApplicationService(db, newTestLogger(), kafka, sheetsSvc, repo, salary)
}

func sampleApplication(now time.Time) *models.Application {
	stageStr := "1"

	return &models.Application{
		ID:             1,
		Company:        "Acme",
		Title:          "Dev",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      now,
		RespondedAt:    &now,
		NextFollowUpAt: &now,
		Stage:          &stageStr,
		Meta:           tools.ToJSONMap(map[string]string{"contacts": "c", "job_description": "jd", "notes": "n"}),
		RowID:          3,
	}
}

func sampleUpdateDTO(now time.Time) dto.UpdateApplicationDTO {
	stage := int64(2)
	amountFrom := 100.0
	amountTo := 200.0

	return dto.UpdateApplicationDTO{
		ID:             1,
		RowID:          3,
		Company:        "NewCo",
		Title:          "Lead",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      now,
		RespondedAt:    &now,
		NextFollowUpAt: &now,
		Stage:          &stage,
		Meta:           map[string]string{"contacts": "c1", "job_description": "jd1", "notes": "n1"},
		Embedding:      []float32{1, 2, 3},
		SalaryApplied: &dto.UpdateApplicationSalaryDTO{
			AmountFrom: &amountFrom,
			AmountTo:   &amountTo,
			Currency:   "USD",
			Period:     enums.SalaryPeriodMonthly,
		},
		SalaryProposed: &dto.UpdateApplicationSalaryDTO{
			AmountFrom: &amountFrom,
			AmountTo:   &amountTo,
			Currency:   "EUR",
			Period:     enums.SalaryPeriodYearly,
		},
	}
}

func TestApplicationServiceAddEmbeddingByID(t *testing.T) {
	t.Parallel()

	application := &models.Application{ID: 1}
	saveErr := errors.New("save failed")
	findErr := errors.New("find failed")

	tests := []struct {
		name        string
		findResult  *models.Application
		findErr     error
		saveErr     error
		expectError bool
	}{
		{
			name:        "repository error",
			findErr:     findErr,
			expectError: true,
		},
		{
			name:        "not found",
			findResult:  nil,
			expectError: true,
		},
		{
			name:        "save error",
			findResult:  application,
			saveErr:     saveErr,
			expectError: true,
		},
		{
			name:       "success",
			findResult: application,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			repo.On("FindByID", mock.Anything, int64(1)).Return(tt.findResult, tt.findErr).Once()
			if tt.findErr == nil && tt.findResult != nil {
				repo.On("Save", mock.Anything, mock.MatchedBy(func(apps []*models.Application) bool {
					if len(apps) != 1 {
						return false
					}
					return apps[0].Embedding != nil
				})).Return(tt.saveErr).Once()
			}

			service := newApplicationService(nil, repo, nil, nil, nil)

			err := service.AddEmbeddingByID(context.Background(), 1, []float32{1, 2, 3})

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			repo.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceUpdate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	repoErr := errors.New("repo failed")

	tests := []struct {
		name        string
		dtoBuilder  func() dto.UpdateApplicationDTO
		setupRepo   func(repo *mocks.ApplicationRepositoryMock)
		db          *gorm.DB
		expectError bool
	}{
		{
			name: "repository find error",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				return sampleUpdateDTO(now)
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(nil, repoErr).Once()
			},
			expectError: true,
		},
		{
			name: "not found",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				return sampleUpdateDTO(now)
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(nil, nil).Once()
			},
			expectError: true,
		},
		{
			name: "invalid employment type",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				updateDTO := sampleUpdateDTO(now)
				updateDTO.EmploymentType = enums.EmploymentType("invalid")

				return updateDTO
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			expectError: true,
		},
		{
			name: "invalid status",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				updateDTO := sampleUpdateDTO(now)
				updateDTO.Status = enums.ApplicationStatus("bad")

				return updateDTO
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			expectError: true,
		},
		{
			name: "invalid work mode",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				updateDTO := sampleUpdateDTO(now)
				updateDTO.WorkMode = enums.WorkMode("bad")

				return updateDTO
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			expectError: true,
		},
		{
			name: "transaction fails",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				return sampleUpdateDTO(now)
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			db:          mockdb.NewUnmigratedDB(t),
			expectError: true,
		},
		{
			name: "success updates salaries",
			dtoBuilder: func() dto.UpdateApplicationDTO {
				return sampleUpdateDTO(now)
			},
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			db: mockdb.NewTestDB(t),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := tt.db
			if db == nil {
				db = mockdb.NewTestDB(t)
			}

			repo := &mocks.ApplicationRepositoryMock{}
			tt.setupRepo(repo)

			service := newApplicationService(db, repo, nil, nil, nil)
			dto := tt.dtoBuilder()

			err := service.Update(context.Background(), dto)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			repo.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceUpdateAndSync(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name        string
		setupRepo   func(repo *mocks.ApplicationRepositoryMock)
		setupSheets func(sheetsSvc *mocks.SheetsServiceMock, dto dto.UpdateApplicationDTO)
		db          *gorm.DB
		expectError bool
	}{
		{
			name: "update error bubbles up",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(nil, errors.New("find err")).Once()
			},
			expectError: true,
		},
		{
			name: "sync error bubbles up",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			setupSheets: func(sheetsSvc *mocks.SheetsServiceMock, updateDTO dto.UpdateApplicationDTO) {
				sheetsSvc.On("GetApplicationFromRow", mock.Anything, int64(3)).
					Return(nil, errors.New("sheet err")).Once()
			},
			db:          mockdb.NewTestDB(t),
			expectError: true,
		},
		{
			name: "success",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			setupSheets: func(sheetsSvc *mocks.SheetsServiceMock, updateDTO dto.UpdateApplicationDTO) {
				sheetsSvc.On("GetApplicationFromRow", mock.Anything, int64(3)).
					Return(&dto.SheetApplicationDTO{
						Company:        "OldCo",
						Title:          "Old",
						EmploymentType: enums.EmploymentTypeProject,
						WorkMode:       enums.WorkModeHybrid,
						Status:         enums.ApplicationStatusDenied,
						AppliedAt:      updateDTO.AppliedAt.AddDate(0, 0, -1),
						RespondedAt:    nil,
						NextFollowUpAt: nil,
						Stage:          nil,
						Meta:           map[string]string{"contacts": "old", "job_description": "old", "notes": "old"},
					}, nil).Once()
				// respond to execSyncFromDB calls with no-ops
				sheetsSvc.On("SetCompany", mock.Anything, int64(3), "NewCo").Return(nil)
				sheetsSvc.On("SetTitle", mock.Anything, int64(3), "Lead").Return(nil)
				sheetsSvc.On("SetEmploymentType", mock.Anything, int64(3), enums.EmploymentTypeFullTime).Return(nil)
				sheetsSvc.On("SetWorkMode", mock.Anything, int64(3), enums.WorkModeRemote).Return(nil)
				sheetsSvc.On("SetStatus", mock.Anything, int64(3), enums.ApplicationStatusApplied).Return(nil)
				sheetsSvc.On("SetAppliedAt", mock.Anything, int64(3), updateDTO.AppliedAt).Return(nil)
				sheetsSvc.On("SetRespondedAt", mock.Anything, int64(3), updateDTO.RespondedAt).Return(nil)
				sheetsSvc.On("SetNextFollowUpAt", mock.Anything, int64(3), updateDTO.NextFollowUpAt).Return(nil)
				sheetsSvc.On("SetStage", mock.Anything, int64(3), updateDTO.Stage).Return(nil)
				sheetsSvc.On("SetContacts", mock.Anything, int64(3), "c1").Return(nil)
				sheetsSvc.On("SetJobDescription", mock.Anything, int64(3), "jd1").Return(nil)
				sheetsSvc.On("SetNotes", mock.Anything, int64(3), "n1").Return(nil)
				sheetsSvc.On("SetSalaryApplied", mock.Anything, int64(3), mock.Anything).Return(nil)
				sheetsSvc.On("SetSalaryProposed", mock.Anything, int64(3), mock.Anything).Return(nil)
			},
			db: mockdb.NewTestDB(t),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := tt.db
			if db == nil {
				db = mockdb.NewTestDB(t)
			}

			updateDTO := sampleUpdateDTO(now)
			repo := &mocks.ApplicationRepositoryMock{}
			tt.setupRepo(repo)

			sheetsSvc := &mocks.SheetsServiceMock{}
			if tt.setupSheets != nil {
				tt.setupSheets(sheetsSvc, updateDTO)
			}

			service := newApplicationService(db, repo, sheetsSvc, nil, nil)

			err := service.UpdateAndSync(context.Background(), updateDTO)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceSyncFromDTO(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	updateDTO := sampleUpdateDTO(now)
	allowAllSetters := func(sheetsSvc *mocks.SheetsServiceMock, update dto.UpdateApplicationDTO) {
		sheetsSvc.On("SetTitle", mock.Anything, int64(3), update.Title).Return(nil).Maybe()
		sheetsSvc.On("SetEmploymentType", mock.Anything, int64(3), enums.EmploymentType(update.EmploymentType)).Return(nil).Maybe()
		sheetsSvc.On("SetWorkMode", mock.Anything, int64(3), enums.WorkMode(update.WorkMode)).Return(nil).Maybe()
		sheetsSvc.On("SetStatus", mock.Anything, int64(3), enums.ApplicationStatus(update.Status)).Return(nil).Maybe()
		sheetsSvc.On("SetAppliedAt", mock.Anything, int64(3), update.AppliedAt).Return(nil).Maybe()
		sheetsSvc.On("SetRespondedAt", mock.Anything, int64(3), update.RespondedAt).Return(nil).Maybe()
		sheetsSvc.On("SetNextFollowUpAt", mock.Anything, int64(3), update.NextFollowUpAt).Return(nil).Maybe()
		sheetsSvc.On("SetStage", mock.Anything, int64(3), update.Stage).Return(nil).Maybe()
		sheetsSvc.On("SetContacts", mock.Anything, int64(3), update.Meta["contacts"]).Return(nil).Maybe()
		sheetsSvc.On("SetJobDescription", mock.Anything, int64(3), update.Meta["job_description"]).Return(nil).Maybe()
		sheetsSvc.On("SetNotes", mock.Anything, int64(3), update.Meta["notes"]).Return(nil).Maybe()
		sheetsSvc.On("SetSalaryApplied", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
		sheetsSvc.On("SetSalaryProposed", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
	}

	tests := []struct {
		name        string
		setupSheets func(sheetsSvc *mocks.SheetsServiceMock)
		expectError bool
	}{
		{
			name: "get application fails",
			setupSheets: func(sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(nil, errors.New("sheet fail")).Once()
			},
			expectError: true,
		},
		{
			name: "sync fails",
			setupSheets: func(sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationFromRow", mock.Anything, int64(3)).
					Return(&dto.SheetApplicationDTO{Company: "c", Title: "t", EmploymentType: enums.EmploymentTypeFullTime, WorkMode: enums.WorkModeRemote, Status: enums.ApplicationStatusApplied, AppliedAt: now, Meta: map[string]string{}}, nil).Once()
				allowAllSetters(sheetsSvc, updateDTO)
				sheetsSvc.On("SetCompany", mock.Anything, int64(3), "NewCo").Return(errors.New("set fail")).Once()
			},
			expectError: true,
		},
		{
			name: "success executes sets",
			setupSheets: func(sheetsSvc *mocks.SheetsServiceMock) {
				appliedAt := updateDTO.AppliedAt.Add(-time.Hour)
				respondedAt := updateDTO.RespondedAt.Add(-time.Minute)
				nextFollowUp := updateDTO.NextFollowUpAt.Add(-time.Minute)
				stage := tools.ToPtr(int64(5))
				sheetsSvc.On("GetApplicationFromRow", mock.Anything, int64(3)).
					Return(&dto.SheetApplicationDTO{
						Company:        "Old",
						Title:          "OldT",
						EmploymentType: enums.EmploymentTypeProject,
						WorkMode:       enums.WorkModeHybrid,
						Status:         enums.ApplicationStatusDenied,
						AppliedAt:      appliedAt,
						RespondedAt:    &respondedAt,
						NextFollowUpAt: &nextFollowUp,
						Stage:          stage,
						Meta: map[string]string{
							"contacts":        "old",
							"job_description": "oldjd",
							"notes":           "oldn",
						},
						SalaryApplied: &dto.SheetApplicationSalaryDTO{
							AmountFrom: tools.ToPtr(float64(1)),
							AmountTo:   tools.ToPtr(float64(2)),
							Currency:   "USD",
							Period:     enums.SalaryPeriodMonthly,
						},
						SalaryProposed: &dto.SheetApplicationSalaryDTO{
							AmountFrom: tools.ToPtr(float64(3)),
							AmountTo:   tools.ToPtr(float64(4)),
							Currency:   "EUR",
							Period:     enums.SalaryPeriodYearly,
						},
					}, nil).Once()

				sheetsSvc.On("SetCompany", mock.Anything, int64(3), "NewCo").Return(nil)
				sheetsSvc.On("SetTitle", mock.Anything, int64(3), "Lead").Return(nil).Maybe()
				sheetsSvc.On("SetEmploymentType", mock.Anything, int64(3), enums.EmploymentTypeFullTime).Return(nil).Maybe()
				sheetsSvc.On("SetWorkMode", mock.Anything, int64(3), enums.WorkModeRemote).Return(nil).Maybe()
				sheetsSvc.On("SetStatus", mock.Anything, int64(3), enums.ApplicationStatusApplied).Return(nil).Maybe()
				sheetsSvc.On("SetAppliedAt", mock.Anything, int64(3), updateDTO.AppliedAt).Return(nil).Maybe()
				sheetsSvc.On("SetRespondedAt", mock.Anything, int64(3), updateDTO.RespondedAt).Return(nil).Maybe()
				sheetsSvc.On("SetNextFollowUpAt", mock.Anything, int64(3), updateDTO.NextFollowUpAt).Return(nil).Maybe()
				sheetsSvc.On("SetStage", mock.Anything, int64(3), updateDTO.Stage).Return(nil).Maybe()
				sheetsSvc.On("SetContacts", mock.Anything, int64(3), "c1").Return(nil).Maybe()
				sheetsSvc.On("SetJobDescription", mock.Anything, int64(3), "jd1").Return(nil).Maybe()
				sheetsSvc.On("SetNotes", mock.Anything, int64(3), "n1").Return(nil).Maybe()
				sheetsSvc.On("SetSalaryApplied", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
				sheetsSvc.On("SetSalaryProposed", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sheetsSvc := &mocks.SheetsServiceMock{}
			tt.setupSheets(sheetsSvc)

			service := newApplicationService(mockdb.NewTestDB(t), &mocks.ApplicationRepositoryMock{}, sheetsSvc, nil, nil)

			err := service.SyncFromDTO(context.Background(), updateDTO)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			sheetsSvc.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceExecSyncFromDBBranches(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	responded := now.Add(time.Hour)
	nextFollowUp := now.Add(2 * time.Hour)
	stage := int64(2)

	updateDTO := dto.UpdateApplicationDTO{
		RowID:          10,
		Company:        "New",
		Title:          "Role",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      now,
		RespondedAt:    &responded,
		NextFollowUpAt: &nextFollowUp,
		Stage:          &stage,
		Meta: map[string]string{
			"contacts":        "c",
			"job_description": "jd",
			"notes":           "n",
		},
		SalaryApplied: &dto.UpdateApplicationSalaryDTO{
			AmountFrom: tools.ToPtr(float64(1)),
			AmountTo:   tools.ToPtr(float64(2)),
			Currency:   "USD",
			Period:     enums.SalaryPeriodMonthly,
		},
		SalaryProposed: &dto.UpdateApplicationSalaryDTO{
			AmountFrom: tools.ToPtr(float64(3)),
			AmountTo:   tools.ToPtr(float64(4)),
			Currency:   "EUR",
			Period:     enums.SalaryPeriodYearly,
		},
	}

	sheetDTO := &dto.SheetApplicationDTO{
		Company:        "Old",
		Title:          "Old",
		EmploymentType: enums.EmploymentTypeProject,
		WorkMode:       enums.WorkModeHybrid,
		Status:         enums.ApplicationStatusDenied,
		AppliedAt:      now.AddDate(0, 0, -1),
		RespondedAt:    nil,
		NextFollowUpAt: nil,
		Stage:          nil,
		Meta: map[string]string{
			"contacts":        "old",
			"job_description": "old",
			"notes":           "old",
		},
	}
	registerExecDefaults := func(svc *mocks.SheetsServiceMock, update dto.UpdateApplicationDTO) {
		svc.On("SetTitle", mock.Anything, update.RowID, update.Title).Return(nil).Maybe()
		svc.On("SetEmploymentType", mock.Anything, update.RowID, enums.EmploymentType(update.EmploymentType)).Return(nil).Maybe()
		svc.On("SetWorkMode", mock.Anything, update.RowID, enums.WorkMode(update.WorkMode)).Return(nil).Maybe()
		svc.On("SetStatus", mock.Anything, update.RowID, enums.ApplicationStatus(update.Status)).Return(nil).Maybe()
		svc.On("SetAppliedAt", mock.Anything, update.RowID, update.AppliedAt).Return(nil).Maybe()
		svc.On("SetRespondedAt", mock.Anything, update.RowID, update.RespondedAt).Return(nil).Maybe()
		svc.On("SetNextFollowUpAt", mock.Anything, update.RowID, update.NextFollowUpAt).Return(nil).Maybe()
		svc.On("SetStage", mock.Anything, update.RowID, update.Stage).Return(nil).Maybe()
		svc.On("SetContacts", mock.Anything, update.RowID, update.Meta["contacts"]).Return(nil).Maybe()
		svc.On("SetJobDescription", mock.Anything, update.RowID, update.Meta["job_description"]).Return(nil).Maybe()
		svc.On("SetNotes", mock.Anything, update.RowID, update.Meta["notes"]).Return(nil).Maybe()
		svc.On("SetSalaryApplied", mock.Anything, update.RowID, mock.Anything).Return(nil).Maybe()
		svc.On("SetSalaryProposed", mock.Anything, update.RowID, mock.Anything).Return(nil).Maybe()
	}

	tests := []struct {
		name        string
		updateDTO   dto.UpdateApplicationDTO
		sheetDTO    *dto.SheetApplicationDTO
		setupSheets func(svc *mocks.SheetsServiceMock, updateDTO dto.UpdateApplicationDTO, sheetDTO *dto.SheetApplicationDTO)
		expectErr   bool
	}{
		{
			name:      "differences updated",
			updateDTO: updateDTO,
			sheetDTO:  sheetDTO,
			setupSheets: func(svc *mocks.SheetsServiceMock, currentDTO dto.UpdateApplicationDTO, _ *dto.SheetApplicationDTO) {
				svc.On("SetCompany", mock.Anything, int64(10), "New").Return(nil)
				svc.On("SetTitle", mock.Anything, int64(10), "Role").Return(nil)
				svc.On("SetEmploymentType", mock.Anything, int64(10), enums.EmploymentTypeFullTime).Return(nil)
				svc.On("SetWorkMode", mock.Anything, int64(10), enums.WorkModeRemote).Return(nil)
				svc.On("SetStatus", mock.Anything, int64(10), enums.ApplicationStatusApplied).Return(nil)
				svc.On("SetAppliedAt", mock.Anything, int64(10), currentDTO.AppliedAt).Return(nil)
				svc.On("SetRespondedAt", mock.Anything, int64(10), currentDTO.RespondedAt).Return(nil)
				svc.On("SetNextFollowUpAt", mock.Anything, int64(10), currentDTO.NextFollowUpAt).Return(nil)
				svc.On("SetStage", mock.Anything, int64(10), currentDTO.Stage).Return(nil)
				svc.On("SetContacts", mock.Anything, int64(10), "c").Return(nil)
				svc.On("SetJobDescription", mock.Anything, int64(10), "jd").Return(nil)
				svc.On("SetNotes", mock.Anything, int64(10), "n").Return(nil)
				svc.On("SetSalaryApplied", mock.Anything, int64(10), mock.Anything).Return(nil)
				svc.On("SetSalaryProposed", mock.Anything, int64(10), mock.Anything).Return(nil)
			},
		},
		{
			name: "salary branches",
			updateDTO: dto.UpdateApplicationDTO{
				RowID:     11,
				Company:   "Same",
				Title:     "Same",
				Meta:      map[string]string{},
				AppliedAt: now,
			},
			sheetDTO: &dto.SheetApplicationDTO{
				Company:        "Same",
				Title:          "Same",
				AppliedAt:      now,
				RespondedAt:    tools.ToPtr(now.Add(time.Minute)),
				NextFollowUpAt: tools.ToPtr(now.Add(2 * time.Minute)),
				SalaryApplied: &dto.SheetApplicationSalaryDTO{
					AmountFrom: tools.ToPtr(float64(10)),
					Currency:   "USD",
					Period:     enums.SalaryPeriodMonthly,
				},
				SalaryProposed: &dto.SheetApplicationSalaryDTO{
					AmountFrom: tools.ToPtr(float64(20)),
					Currency:   "USD",
					Period:     enums.SalaryPeriodMonthly,
				},
			},
			setupSheets: func(svc *mocks.SheetsServiceMock, _ dto.UpdateApplicationDTO, _ *dto.SheetApplicationDTO) {
				svc.On("SetSalaryApplied", mock.Anything, int64(11), (*dto.SalaryDataDTO)(nil)).Return(nil)
				svc.On("SetSalaryProposed", mock.Anything, int64(11), (*dto.SalaryDataDTO)(nil)).Return(nil)
				svc.On("SetRespondedAt", mock.Anything, int64(11), (*time.Time)(nil)).Return(nil)
				svc.On("SetNextFollowUpAt", mock.Anything, int64(11), (*time.Time)(nil)).Return(nil)
			},
		},
		{
			name:      "group wait error surfaces",
			updateDTO: updateDTO,
			sheetDTO:  sheetDTO,
			setupSheets: func(svc *mocks.SheetsServiceMock, _ dto.UpdateApplicationDTO, _ *dto.SheetApplicationDTO) {
				svc.On("SetCompany", mock.Anything, int64(10), "New").Return(errors.New("set error")).Once()
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svcMock := &mocks.SheetsServiceMock{}
			svcMock.On("GetApplicationFromRow", mock.Anything, tt.updateDTO.RowID).Return(tt.sheetDTO, nil).Once()
			if tt.setupSheets != nil {
				tt.setupSheets(svcMock, tt.updateDTO, tt.sheetDTO)
			}
			registerExecDefaults(svcMock, tt.updateDTO)

			service := newApplicationService(mockdb.NewTestDB(t), &mocks.ApplicationRepositoryMock{}, svcMock, nil, nil)

			err := service.SyncFromDTO(context.Background(), tt.updateDTO)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			svcMock.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceSyncFromDTOSafetyLock(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	updateDTO := sampleUpdateDTO(now)
	updateDTO.RowID = 20

	sheetDTO := &dto.SheetApplicationDTO{
		Company:        "Old",
		Title:          "Old",
		EmploymentType: enums.EmploymentTypeProject,
		WorkMode:       enums.WorkModeHybrid,
		Status:         enums.ApplicationStatusDenied,
		AppliedAt:      now.AddDate(0, 0, -1),
		RespondedAt:    nil,
		NextFollowUpAt: nil,
		Stage:          tools.ToPtr(int64(1)),
		Meta:           map[string]string{"contacts": "old", "job_description": "old", "notes": "old"},
	}

	sheetsSvc := &mocks.SheetsServiceMock{}
	sheetsSvc.On("GetApplicationFromRow", mock.Anything, updateDTO.RowID).Return(sheetDTO, nil).Twice()

	var companyCalls atomic.Int32
	sheetsSvc.On("SetCompany", mock.Anything, updateDTO.RowID, updateDTO.Company).
		Run(func(args mock.Arguments) {
			if companyCalls.Add(1) == 1 {
				block := make(chan struct{})
				go func() {
					time.Sleep(30 * time.Millisecond)
					close(block)
				}()
				<-block
			}
		}).
		Return(nil).Twice()
	sheetsSvc.On("SetTitle", mock.Anything, updateDTO.RowID, updateDTO.Title).Return(nil).Twice()
	sheetsSvc.On("SetEmploymentType", mock.Anything, updateDTO.RowID, updateDTO.EmploymentType).Return(nil).Twice()
	sheetsSvc.On("SetWorkMode", mock.Anything, updateDTO.RowID, updateDTO.WorkMode).Return(nil).Twice()
	sheetsSvc.On("SetStatus", mock.Anything, updateDTO.RowID, updateDTO.Status).Return(nil).Twice()
	sheetsSvc.On("SetAppliedAt", mock.Anything, updateDTO.RowID, updateDTO.AppliedAt).Return(nil).Twice()
	sheetsSvc.On("SetRespondedAt", mock.Anything, updateDTO.RowID, updateDTO.RespondedAt).Return(nil).Twice()
	sheetsSvc.On("SetNextFollowUpAt", mock.Anything, updateDTO.RowID, updateDTO.NextFollowUpAt).Return(nil).Twice()
	sheetsSvc.On("SetStage", mock.Anything, updateDTO.RowID, updateDTO.Stage).Return(nil).Twice()
	sheetsSvc.On("SetContacts", mock.Anything, updateDTO.RowID, updateDTO.Meta["contacts"]).Return(nil).Twice()
	sheetsSvc.On("SetJobDescription", mock.Anything, updateDTO.RowID, updateDTO.Meta["job_description"]).Return(nil).Twice()
	sheetsSvc.On("SetNotes", mock.Anything, updateDTO.RowID, updateDTO.Meta["notes"]).Return(nil).Twice()
	sheetsSvc.On("SetSalaryApplied", mock.Anything, updateDTO.RowID, mock.Anything).Return(nil).Twice()
	sheetsSvc.On("SetSalaryProposed", mock.Anything, updateDTO.RowID, mock.Anything).Return(nil).Twice()

	service := newApplicationService(mockdb.NewTestDB(t), &mocks.ApplicationRepositoryMock{}, sheetsSvc, nil, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	var err1, err2 error
	go func() {
		defer wg.Done()
		err1 = service.SyncFromDTO(context.Background(), updateDTO)
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		defer wg.Done()
		err2 = service.SyncFromDTO(context.Background(), updateDTO)
	}()

	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)
	sheetsSvc.AssertExpectations(t)
}

func TestApplicationServiceGetApplicationDiff(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	baseSheet := dto.SheetApplicationDTO{
		Company:        "New",
		Title:          "Lead",
		EmploymentType: enums.EmploymentTypeProject,
		WorkMode:       enums.WorkModeHybrid,
		Status:         enums.ApplicationStatusDenied,
		AppliedAt:      now.AddDate(0, 0, 1),
		RespondedAt:    tools.ToPtr(now.Add(2 * time.Hour)),
		NextFollowUpAt: tools.ToPtr(now.Add(3 * time.Hour)),
		Stage:          tools.ToPtr(int64(5)),
		Meta:           map[string]string{"contacts": "c2", "job_description": "jd2", "notes": "n2"},
		SalaryApplied: &dto.SheetApplicationSalaryDTO{
			AmountFrom: tools.ToPtr(float64(10)),
			AmountTo:   tools.ToPtr(float64(20)),
			Currency:   "USD",
			Period:     enums.SalaryPeriodMonthly,
		},
		SalaryProposed: &dto.SheetApplicationSalaryDTO{
			AmountFrom: tools.ToPtr(float64(30)),
			AmountTo:   tools.ToPtr(float64(40)),
			Currency:   "EUR",
			Period:     enums.SalaryPeriodYearly,
		},
	}

	makeDBApp := func() *models.Application {
		return &models.Application{
			ID:             1,
			Company:        "Acme",
			Title:          "Dev",
			EmploymentType: enums.EmploymentTypeFullTime,
			WorkMode:       enums.WorkModeRemote,
			Status:         enums.ApplicationStatusApplied,
			AppliedAt:      now,
			RespondedAt:    tools.ToPtr(now),
			NextFollowUpAt: tools.ToPtr(now.Add(time.Hour)),
			Stage:          tools.ToPtr("3"),
			Meta:           tools.ToJSONMap(map[string]string{"contacts": "c", "job_description": "jd", "notes": "n"}),
			RowID:          5,
			SalaryApplied: &models.Salary{
				AmountFrom: tools.ToPtr(float64(1)),
				AmountTo:   tools.ToPtr(float64(2)),
				Currency:   "USD",
				Period:     enums.SalaryPeriodMonthly,
			},
			SalaryProposed: &models.Salary{
				AmountFrom: tools.ToPtr(float64(3)),
				AmountTo:   tools.ToPtr(float64(4)),
				Currency:   "EUR",
				Period:     enums.SalaryPeriodYearly,
			},
		}
	}

	tests := []struct {
		name      string
		rowID     int64
		setupRepo func(repo *mocks.ApplicationRepositoryMock)
		expectErr bool
		expectNil bool
		assertion func(t *testing.T, diffs []dto.ApplicationDiffEntry)
	}{
		{
			name:  "sheet only row skipped when not in db",
			rowID: 5,
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByRowID", mock.Anything, int64(5)).Return(nil, nil).Once()
			},
			expectNil: true,
		},
		{
			name:  "differences returned for mismatched fields",
			rowID: 6,
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByRowID", mock.Anything, int64(6)).Return(makeDBApp(), nil).Once()
			},
			assertion: func(t *testing.T, diffs []dto.ApplicationDiffEntry) {
				require.Len(t, diffs, 1)
				assert.Greater(t, len(diffs[0].Differences), 5)
			},
		},
		{
			name:  "stage parse error bubbles up",
			rowID: 7,
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				app := makeDBApp()
				badStage := "bad"
				app.Stage = &badStage
				repo.On("FindByRowID", mock.Anything, int64(7)).Return(app, nil).Once()
			},
			expectErr: true,
		},
		{
			name:  "salary nil vs value is captured",
			rowID: 8,
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				app := makeDBApp()
				app.RowID = 8
				app.SalaryApplied = nil
				app.SalaryProposed = nil
				repo.On("FindByRowID", mock.Anything, int64(8)).Return(app, nil).Once()
			},
			assertion: func(t *testing.T, diffs []dto.ApplicationDiffEntry) {
				require.Len(t, diffs, 1)
				assert.GreaterOrEqual(t, len(diffs[0].Differences), 2)
			},
		},
		{
			name:  "stage exists only in sheet",
			rowID: 9,
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				app := makeDBApp()
				app.RowID = 9
				app.Stage = nil
				repo.On("FindByRowID", mock.Anything, int64(9)).Return(app, nil).Once()
			},
			assertion: func(t *testing.T, diffs []dto.ApplicationDiffEntry) {
				require.Len(t, diffs, 1)
				found := false
				for _, d := range diffs[0].Differences {
					if d.Field == "stage" {
						found = true
					}
				}
				assert.True(t, found)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			tt.setupRepo(repo)

			sheetsSvc := &mocks.SheetsServiceMock{}
			sheetsSvc.On("GetApplicationsFromRows", mock.Anything, tt.rowID, tt.rowID).
				Return(map[int64]dto.SheetApplicationDTO{tt.rowID: baseSheet}, nil).Once()

			service := newApplicationService(mockdb.NewTestDB(t), repo, sheetsSvc, nil, nil)

			diffs, _, err := service.GetApplicationsDiff(context.Background(), tt.rowID, tt.rowID)

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.expectNil {
				assert.Nil(t, diffs)
			} else {
				require.NotNil(t, diffs)
				if tt.assertion != nil {
					tt.assertion(t, diffs)
				}
			}

			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceGetApplicationsDiff(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	sheetApplication := dto.SheetApplicationDTO{
		Company:        "Acme",
		Title:          "Dev",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         enums.ApplicationStatusApplied,
		AppliedAt:      now,
		Meta:           map[string]string{},
	}

	tests := []struct {
		name        string
		start       int64
		end         int64
		setupMocks  func(repo *mocks.ApplicationRepositoryMock, sheetsSvc *mocks.SheetsServiceMock)
		expectErr   bool
		expectEmpty bool
	}{
		{
			name:      "start below minimal",
			start:     1,
			end:       5,
			expectErr: true,
		},
		{
			name:      "end lower than start",
			start:     5,
			end:       2,
			expectErr: true,
		},
		{
			name:  "sheets error",
			start: 3,
			end:   3,
			setupMocks: func(repo *mocks.ApplicationRepositoryMock, sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(3)).
					Return(nil, errors.New("sheet err")).Once()
			},
			expectErr: true,
		},
		{
			name:  "repository error",
			start: 4,
			end:   4,
			setupMocks: func(repo *mocks.ApplicationRepositoryMock, sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationsFromRows", mock.Anything, int64(4), int64(4)).
					Return(map[int64]dto.SheetApplicationDTO{4: sheetApplication}, nil).Once()
				repo.On("FindByRowID", mock.Anything, int64(4)).Return(nil, errors.New("find err")).Once()
			},
			expectErr: true,
		},
		{
			name:  "no differences",
			start: 5,
			end:   5,
			setupMocks: func(repo *mocks.ApplicationRepositoryMock, sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationsFromRows", mock.Anything, int64(5), int64(5)).
					Return(map[int64]dto.SheetApplicationDTO{5: sheetApplication}, nil).Once()
				repo.On("FindByRowID", mock.Anything, int64(5)).Return(&models.Application{
					ID:             10,
					Company:        sheetApplication.Company,
					Title:          sheetApplication.Title,
					EmploymentType: sheetApplication.EmploymentType,
					WorkMode:       sheetApplication.WorkMode,
					Status:         sheetApplication.Status,
					AppliedAt:      sheetApplication.AppliedAt,
					Meta:           tools.ToJSONMap(sheetApplication.Meta),
					RowID:          5,
				}, nil).Once()
			},
			expectEmpty: true,
		},
		{
			name:  "diff returned",
			start: 6,
			end:   6,
			setupMocks: func(repo *mocks.ApplicationRepositoryMock, sheetsSvc *mocks.SheetsServiceMock) {
				sheetsSvc.On("GetApplicationsFromRows", mock.Anything, int64(6), int64(6)).
					Return(map[int64]dto.SheetApplicationDTO{6: sheetApplication}, nil).Once()
				app := &models.Application{
					ID:             11,
					Company:        "Different",
					Title:          "Dev",
					EmploymentType: sheetApplication.EmploymentType,
					WorkMode:       sheetApplication.WorkMode,
					Status:         sheetApplication.Status,
					AppliedAt:      sheetApplication.AppliedAt,
					Meta:           tools.ToJSONMap(map[string]string{"contacts": "c"}),
					RowID:          6,
				}
				repo.On("FindByRowID", mock.Anything, int64(6)).Return(app, nil).Once()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			sheetsSvc := &mocks.SheetsServiceMock{}
			if tt.setupMocks != nil {
				tt.setupMocks(repo, sheetsSvc)
			}

			service := newApplicationService(mockdb.NewTestDB(t), repo, sheetsSvc, nil, nil)

			diffs, rowsChecked, err := service.GetApplicationsDiff(context.Background(), tt.start, tt.end)

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, rowsChecked)

			if tt.expectEmpty {
				assert.Nil(t, diffs)
			} else {
				assert.NotEmpty(t, diffs)
			}

			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceGetMaxRowID(t *testing.T) {
	t.Parallel()

	repo := &mocks.ApplicationRepositoryMock{}
	rowID := int64(5)
	repo.On("GetMaxRowID", mock.Anything).Return(&rowID, nil).Once()

	service := newApplicationService(nil, repo, nil, nil, nil)

	result, err := service.GetMaxRowID(context.Background())

	require.NoError(t, err)
	assert.Equal(t, &rowID, result)
	repo.AssertExpectations(t)
}

func TestApplicationServiceFetch(t *testing.T) {
	t.Parallel()

	os.Setenv(services.KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING, "topic")

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name        string
		setupRepo   func(repo *mocks.ApplicationRepositoryMock)
		setupSheets func(svc *mocks.SheetsServiceMock)
		setupKafka  func(kafka *mocks.KafkaPublisherMock)
		db          *gorm.DB
		expectErr   bool
		expectZero  bool
	}{
		{
			name: "get max row fails",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("GetMaxRowID", mock.Anything).Return(nil, errors.New("max err")).Once()
			},
			expectErr: true,
		},
		{
			name: "sheets error",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				row := int64(2)
				repo.On("GetMaxRowID", mock.Anything).Return(&row, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(sheets.LastRow)).
					Return(nil, errors.New("sheet err")).Once()
			},
			expectErr: true,
		},
		{
			name: "no applications",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				row := int64(2)
				repo.On("GetMaxRowID", mock.Anything).Return(&row, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(sheets.LastRow)).
					Return(map[int64]dto.SheetApplicationDTO{}, nil).Once()
			},
			expectZero: true,
		},
		{
			name: "reset max row and transaction failure",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("GetMaxRowID", mock.Anything).Return(tools.ToPtr(int64(0)), nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(sheets.LastRow)).
					Return(map[int64]dto.SheetApplicationDTO{
						3: {
							Company:        "Acme",
							Title:          "Dev",
							EmploymentType: enums.EmploymentTypeFullTime,
							WorkMode:       enums.WorkModeRemote,
							Status:         enums.ApplicationStatusApplied,
							AppliedAt:      now,
							Meta:           map[string]string{},
						},
					}, nil).Once()
			},
			db:        mockdb.NewUnmigratedDB(t),
			expectErr: true,
		},
		{
			name: "publish error surfaces",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				row := int64(2)
				repo.On("GetMaxRowID", mock.Anything).Return(&row, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(sheets.LastRow)).
					Return(map[int64]dto.SheetApplicationDTO{
						3: {
							Company:        "Acme",
							Title:          "Dev",
							EmploymentType: enums.EmploymentTypeFullTime,
							WorkMode:       enums.WorkModeRemote,
							Status:         enums.ApplicationStatusApplied,
							AppliedAt:      now,
							Meta:           map[string]string{},
						},
					}, nil).Once()
			},
			setupKafka: func(kafka *mocks.KafkaPublisherMock) {
				kafka.On("Publish", mock.Anything, "topic", mock.Anything, mock.Anything).
					Return(errors.New("publish err")).Once()
			},
			db:        mockdb.NewTestDB(t),
			expectErr: true,
		},
		{
			name: "success saves data",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				row := int64(2)
				repo.On("GetMaxRowID", mock.Anything).Return(&row, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationsFromRows", mock.Anything, int64(3), int64(sheets.LastRow)).
					Return(map[int64]dto.SheetApplicationDTO{
						3: {
							Company:        "Acme",
							Title:          "Dev",
							EmploymentType: enums.EmploymentTypeFullTime,
							WorkMode:       enums.WorkModeRemote,
							Status:         enums.ApplicationStatusApplied,
							AppliedAt:      now,
							Meta:           map[string]string{},
							SalaryApplied: &dto.SheetApplicationSalaryDTO{
								AmountFrom: tools.ToPtr(float64(1)),
								Currency:   "USD",
								Period:     enums.SalaryPeriodMonthly,
							},
							SalaryProposed: &dto.SheetApplicationSalaryDTO{
								AmountFrom: tools.ToPtr(float64(5)),
								AmountTo:   tools.ToPtr(float64(6)),
								Currency:   "EUR",
								Period:     enums.SalaryPeriodYearly,
							},
						},
					}, nil).Once()
			},
			setupKafka: func(kafka *mocks.KafkaPublisherMock) {
				kafka.On("Publish", mock.Anything, "topic", mock.Anything, mock.Anything).
					Return(nil).Once()
			},
			db: mockdb.NewTestDB(t),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}

			sheetsSvc := &mocks.SheetsServiceMock{}
			if tt.setupSheets != nil {
				tt.setupSheets(sheetsSvc)
			}

			kafka := &mocks.KafkaPublisherMock{}
			if tt.setupKafka != nil {
				tt.setupKafka(kafka)
			}

			db := tt.db
			if db == nil {
				db = mockdb.NewTestDB(t)
			}

			service := newApplicationService(db, repo, sheetsSvc, kafka, nil)

			appCount, salaryCount, err := service.Fetch(context.Background())

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expectZero {
					assert.Equal(t, int64(0), appCount)
					assert.Equal(t, int64(0), salaryCount)
				} else {
					assert.Greater(t, appCount, int64(0))
				}
			}

			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
			kafka.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceSyncFromSheet(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name        string
		setupRepo   func(repo *mocks.ApplicationRepositoryMock)
		setupSheets func(svc *mocks.SheetsServiceMock)
		db          *gorm.DB
		expectErr   bool
	}{
		{
			name: "find by row error",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByRowID", mock.Anything, int64(3)).Return(nil, errors.New("find err")).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(&dto.SheetApplicationDTO{}, nil).Once()
			},
			expectErr: true,
		},
		{
			name: "get application from sheet fails",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(nil, errors.New("sheet err")).Once()
			},
			expectErr: true,
		},
		{
			name: "map fails when not in db",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByRowID", mock.Anything, int64(3)).Return(nil, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(&dto.SheetApplicationDTO{}, nil).Once()
			},
			expectErr: true,
		},
		{
			name: "update fails",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("FindByRowID", mock.Anything, int64(3)).Return(sampleApplication(now), nil).Once()
				repo.On("FindByID", mock.Anything, int64(1)).Return(nil, errors.New("find err")).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(&dto.SheetApplicationDTO{
					Company:        "Acme",
					Title:          "Dev",
					EmploymentType: enums.EmploymentTypeFullTime,
					WorkMode:       enums.WorkModeRemote,
					Status:         enums.ApplicationStatusApplied,
					AppliedAt:      now,
				}, nil).Once()
			},
			db:        mockdb.NewTestDB(t),
			expectErr: true,
		},
		{
			name: "success",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				vec := pgvector.NewVector([]float32{1, 2})
				repo.On("FindByRowID", mock.Anything, int64(3)).Return(&models.Application{
					ID:             1,
					Company:        "Acme",
					Title:          "Dev",
					EmploymentType: enums.EmploymentTypeFullTime,
					WorkMode:       enums.WorkModeRemote,
					Status:         enums.ApplicationStatusApplied,
					AppliedAt:      now,
					RespondedAt:    &now,
					NextFollowUpAt: &now,
					Stage:          tools.ToPtr("1"),
					Meta:           tools.ToJSONMap(map[string]string{"contacts": "c"}),
					RowID:          3,
					Embedding:      &vec,
					SalaryApplied: &models.Salary{
						AmountFrom: tools.ToPtr(float64(1)),
						AmountTo:   tools.ToPtr(float64(2)),
						Currency:   "USD",
						Period:     enums.SalaryPeriodMonthly,
					},
				}, nil).Once()
				repo.On("FindByID", mock.Anything, int64(1)).Return(sampleApplication(now), nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).Return(&dto.SheetApplicationDTO{
					Company:        "Acme",
					Title:          "Dev",
					EmploymentType: enums.EmploymentTypeFullTime,
					WorkMode:       enums.WorkModeRemote,
					Status:         enums.ApplicationStatusApplied,
					AppliedAt:      now,
					RespondedAt:    &now,
					NextFollowUpAt: &now,
					Stage:          tools.ToPtr(int64(1)),
					Meta:           map[string]string{"contacts": "c"},
					SalaryApplied: &dto.SheetApplicationSalaryDTO{
						AmountFrom: tools.ToPtr(float64(1)),
						AmountTo:   tools.ToPtr(float64(2)),
						Currency:   "USD",
						Period:     enums.SalaryPeriodMonthly,
					},
					SalaryProposed: &dto.SheetApplicationSalaryDTO{
						AmountFrom: tools.ToPtr(float64(3)),
						AmountTo:   tools.ToPtr(float64(4)),
						Currency:   "EUR",
						Period:     enums.SalaryPeriodYearly,
					},
				}, nil).Once()
			},
			db: mockdb.NewTestDB(t),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			tt.setupRepo(repo)

			sheetsSvc := &mocks.SheetsServiceMock{}
			tt.setupSheets(sheetsSvc)

			db := tt.db
			if db == nil {
				db = mockdb.NewTestDB(t)
			}

			service := newApplicationService(db, repo, sheetsSvc, nil, nil)

			err := service.SyncFromSheet(context.Background(), 3)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
		})
	}
}

func TestApplicationServiceListAndFindByID(t *testing.T) {
	t.Parallel()

	repo := &mocks.ApplicationRepositoryMock{}
	app := &models.Application{ID: 1}
	repo.On("List", mock.Anything, []enums.ApplicationStatus{}, []enums.ApplicationStatus{}, false).
		Return([]*models.Application{app}, nil).Once()
	repo.On("FindByID", mock.Anything, int64(1)).Return(app, nil).Once()

	service := newApplicationService(nil, repo, nil, nil, nil)

	list, err := service.List(context.Background(), []enums.ApplicationStatus{}, []enums.ApplicationStatus{}, false)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	result, err := service.FindByID(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, app, result)

	repo.AssertExpectations(t)
}

func TestApplicationServiceCleanUpMeetingsInBatches(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Add(-time.Hour)
	appliedAt := time.Now()
	appMeeting := &models.Application{
		ID:             1,
		Company:        "Acme",
		Title:          "Dev",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         enums.ApplicationStatusMeeting,
		AppliedAt:      appliedAt,
		NextFollowUpAt: &now,
		RowID:          3,
	}

	tests := []struct {
		name        string
		setupRepo   func(repo *mocks.ApplicationRepositoryMock)
		setupSheets func(svc *mocks.SheetsServiceMock)
		expectErr   bool
		expectedCnt int64
	}{
		{
			name: "pagination error",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("Paginate", mock.Anything, mock.Anything).Return(nil, errors.New("paginate err")).Once()
			},
			expectErr: true,
		},
		{
			name: "no updates needed",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("Paginate", mock.Anything, mock.Anything).Return(&dto.PaginatedApplications{
					CurrentPage: 1,
					Content: []*models.Application{
						{Status: enums.ApplicationStatusApplied},
					},
				}, nil).Once()
			},
			expectedCnt: 0,
		},
		{
			name: "success updates and syncs",
			setupRepo: func(repo *mocks.ApplicationRepositoryMock) {
				repo.On("Paginate", mock.Anything, dto.PaginationParams{Page: 1, PageSize: 2}).
					Return(&dto.PaginatedApplications{
						CurrentPage: 1,
						Content:     []*models.Application{appMeeting},
					}, nil).Once()
				repo.On("Save", mock.Anything, mock.MatchedBy(func(apps []*models.Application) bool {
					return len(apps) == 1 && apps[0].Status == enums.ApplicationStatusPending
				})).Return(nil).Once()
				repo.On("FindByID", mock.Anything, int64(1)).Return(appMeeting, nil).Once()
			},
			setupSheets: func(svc *mocks.SheetsServiceMock) {
				svc.On("GetApplicationFromRow", mock.Anything, int64(3)).
					Return(&dto.SheetApplicationDTO{
						Company:        "Acme",
						Title:          "Dev",
						EmploymentType: enums.EmploymentTypeFullTime,
						WorkMode:       enums.WorkModeRemote,
						Status:         enums.ApplicationStatusPending,
						AppliedAt:      appliedAt,
						Meta:           map[string]string{},
					}, nil).Once()
				svc.On("SetCompany", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
				svc.On("SetTitle", mock.Anything, int64(3), mock.Anything).Return(nil).Maybe()
				svc.On("SetEmploymentType", mock.Anything, int64(3), enums.EmploymentTypeFullTime).Return(nil).Maybe()
				svc.On("SetWorkMode", mock.Anything, int64(3), enums.WorkModeRemote).Return(nil).Maybe()
				svc.On("SetStatus", mock.Anything, int64(3), enums.ApplicationStatusPending).Return(nil).Maybe()
				svc.On("SetAppliedAt", mock.Anything, int64(3), mock.AnythingOfType("time.Time")).Return(nil).Maybe()
				svc.On("SetRespondedAt", mock.Anything, int64(3), (*time.Time)(nil)).Return(nil).Maybe()
				svc.On("SetNextFollowUpAt", mock.Anything, int64(3), appMeeting.NextFollowUpAt).Return(nil)
				svc.On("SetStage", mock.Anything, int64(3), (*int64)(nil)).Return(nil).Maybe()
				svc.On("SetContacts", mock.Anything, int64(3), "").Return(nil).Maybe()
				svc.On("SetJobDescription", mock.Anything, int64(3), "").Return(nil).Maybe()
				svc.On("SetNotes", mock.Anything, int64(3), "").Return(nil).Maybe()
				svc.On("SetSalaryApplied", mock.Anything, int64(3), (*dto.SalaryDataDTO)(nil)).Return(nil).Maybe()
				svc.On("SetSalaryProposed", mock.Anything, int64(3), (*dto.SalaryDataDTO)(nil)).Return(nil).Maybe()
			},
			expectedCnt: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.ApplicationRepositoryMock{}
			tt.setupRepo(repo)

			sheetsSvc := &mocks.SheetsServiceMock{}
			if tt.setupSheets != nil {
				tt.setupSheets(sheetsSvc)
			}

			service := newApplicationService(mockdb.NewTestDB(t), repo, sheetsSvc, nil, nil)

			updated, err := service.CleanUpMeetingsInBatches(context.Background(), 2)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCnt, updated)
			}

			repo.AssertExpectations(t)
			sheetsSvc.AssertExpectations(t)
		})
	}
}
