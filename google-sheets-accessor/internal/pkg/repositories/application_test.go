package repositories_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/repositories"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools/testhelper/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

func newApplication(company string, status enums.ApplicationStatus, rowID int64, appliedAt time.Time) models.Application {
	return models.Application{
		Company:        company,
		Title:          "Dev",
		EmploymentType: enums.EmploymentTypeFullTime,
		WorkMode:       enums.WorkModeRemote,
		Status:         status,
		AppliedAt:      appliedAt,
		RowID:          rowID,
	}
}

func TestApplicationRepositoryFindByID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name   string
		setup  func(t *testing.T) *repositories.ApplicationRepository
		assert func(t *testing.T, application *models.Application, err error)
	}{
		{
			name: "found with preloaded salaries",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{}, &models.Application{})

				applied := models.Salary{Currency: "USD", Period: enums.SalaryPeriodMonthly}
				require.NoError(t, db.Create(&applied).Error)

				proposed := models.Salary{Currency: "EUR", Period: enums.SalaryPeriodYearly}
				require.NoError(t, db.Create(&proposed).Error)

				stage := "1"
				app := newApplication("Acme", enums.ApplicationStatusApplied, 10, now)
				app.ID = 1
				app.Stage = &stage
				app.SalaryAppliedID = &applied.ID
				app.SalaryProposedID = &proposed.ID

				require.NoError(t, db.Create(&app).Error)

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				require.NoError(t, err)
				require.NotNil(t, application)
				assert.Equal(t, int64(1), application.ID)
				require.NotNil(t, application.SalaryApplied)
				require.NotNil(t, application.SalaryProposed)
				assert.Equal(t, "USD", application.SalaryApplied.Currency)
				assert.Equal(t, "EUR", application.SalaryProposed.Currency)
				require.NotNil(t, application.Stage)
				assert.Equal(t, "1", *application.Stage)
			},
		},
		{
			name: "not found returns nil",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{}, &models.Application{})

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				require.NoError(t, err)
				if application != nil {
					assert.Zero(t, application.ID)
				}
			},
		},
		{
			name: "query error bubbles up",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t)

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				assert.Nil(t, application)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := tt.setup(t)
			application, err := repo.FindByID(context.Background(), 1)

			tt.assert(t, application, err)
		})
	}
}

func TestApplicationRepositoryFindByRowID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name   string
		setup  func(t *testing.T) *repositories.ApplicationRepository
		assert func(t *testing.T, application *models.Application, err error)
	}{
		{
			name: "found by row id",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{}, &models.Application{})
				app := newApplication("RowId", enums.ApplicationStatusApplied, 42, now)
				app.ID = 2

				require.NoError(t, db.Create(&app).Error)

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				require.NoError(t, err)
				require.NotNil(t, application)
				assert.Equal(t, int64(42), application.RowID)
			},
		},
		{
			name: "not found returns nil",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{}, &models.Application{})

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				require.NoError(t, err)
				if application != nil {
					assert.Zero(t, application.ID)
				}
			},
		},
		{
			name: "query error bubbles up",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t)

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			assert: func(t *testing.T, application *models.Application, err error) {
				assert.Nil(t, application)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := tt.setup(t)
			application, err := repo.FindByRowID(context.Background(), 42)

			tt.assert(t, application, err)
		})
	}
}

func TestApplicationRepositorySave(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	stage := "2"

	tests := []struct {
		name        string
		db          *gorm.DB
		application *models.Application
		expectError bool
	}{
		{
			name: "persists application",
			db:   testdb.NewSQLiteTestDB(t, &models.Application{}),
			application: func() *models.Application {
				app := newApplication("SaveCo", enums.ApplicationStatusApplied, 7, now)
				app.Stage = &stage

				return &app
			}(),
		},
		{
			name: "returns error on save failure",
			db:   testdb.NewSQLiteTestDB(t),
			application: func() *models.Application {
				app := newApplication("Broken", enums.ApplicationStatusApplied, 8, now)

				return &app
			}(),
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := repositories.NewApplicationRepository(newTestLogger(), tt.db)

			err := repo.Save(context.Background(), tt.application)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			var stored models.Application
			require.NoError(t, tt.db.First(&stored).Error)
			assert.Equal(t, tt.application.Company, stored.Company)
			assert.Equal(t, tt.application.RowID, stored.RowID)
			require.NotNil(t, stored.Stage)
			assert.Equal(t, *tt.application.Stage, *stored.Stage)
		})
	}
}

func TestApplicationRepositoryGetMaxRowID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name        string
		setup       func(t *testing.T) *repositories.ApplicationRepository
		expect      int64
		expectError bool
	}{
		{
			name: "returns zero when no rows",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Application{})

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			expect: 0,
		},
		{
			name: "returns max row id",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Application{})

				for _, rowID := range []int64{2, 5, 3} {
					app := newApplication("Max", enums.ApplicationStatusApplied, rowID, now)
					require.NoError(t, db.Create(&app).Error)
				}

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			expect: 5,
		},
		{
			name: "returns error on query failure",
			setup: func(t *testing.T) *repositories.ApplicationRepository {
				db := testdb.NewSQLiteTestDB(t)

				return repositories.NewApplicationRepository(newTestLogger(), db)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := tt.setup(t)

			rowID, err := repo.GetMaxRowID(context.Background())

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, rowID)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, rowID)
			assert.Equal(t, tt.expect, *rowID)
		})
	}
}

func TestApplicationRepositoryList(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	appliedEmailID := int64(1)
	deniedEmailID := int64(2)

	insertApplications := func(t *testing.T, db *gorm.DB) {
		withAppliedEmail := newApplication("AppliedEmail", enums.ApplicationStatusApplied, 1, now)
		withAppliedEmail.AppliedEmailReceived = tools.ToPtr(now)
		withAppliedEmail.AppliedEmailID = &appliedEmailID
		require.NoError(t, db.Create(&withAppliedEmail).Error)

		appliedNoEmail := newApplication("AppliedNoEmail", enums.ApplicationStatusApplied, 2, now)
		require.NoError(t, db.Create(&appliedNoEmail).Error)

		deniedWithEmail := newApplication("DeniedEmail", enums.ApplicationStatusDenied, 3, now)
		deniedWithEmail.DeniedEmailReceived = tools.ToPtr(now)
		deniedWithEmail.DeniedEmailID = &deniedEmailID
		require.NoError(t, db.Create(&deniedWithEmail).Error)

		deniedNoEmail := newApplication("DeniedNoEmail", enums.ApplicationStatusDenied, 4, now)
		require.NoError(t, db.Create(&deniedNoEmail).Error)

		offer := newApplication("Offer", enums.ApplicationStatusOffer, 5, now)
		require.NoError(t, db.Create(&offer).Error)
	}

	type expectations struct {
		companies []string
		expectErr bool
	}

	tests := []struct {
		name        string
		include     []enums.ApplicationStatus
		exclude     []enums.ApplicationStatus
		isReply     bool
		configureDB func(db *gorm.DB)
		expect      expectations
	}{
		{
			name:    "excludes statuses when provided",
			exclude: []enums.ApplicationStatus{enums.ApplicationStatusDenied},
			expect: expectations{
				companies: []string{"AppliedEmail", "AppliedNoEmail", "Offer"},
			},
		},
		{
			name:    "filters applied with reply received",
			include: []enums.ApplicationStatus{enums.ApplicationStatusApplied},
			isReply: true,
			expect: expectations{
				companies: []string{"AppliedEmail"},
			},
		},
		{
			name:    "filters applied without reply received",
			include: []enums.ApplicationStatus{enums.ApplicationStatusApplied},
			expect: expectations{
				companies: []string{"AppliedNoEmail"},
			},
		},
		{
			name:    "filters denied with reply received",
			include: []enums.ApplicationStatus{enums.ApplicationStatusDenied},
			isReply: true,
			expect: expectations{
				companies: []string{"DeniedEmail"},
			},
		},
		{
			name:    "filters denied without reply received",
			include: []enums.ApplicationStatus{enums.ApplicationStatusDenied},
			expect: expectations{
				companies: []string{"DeniedNoEmail"},
			},
		},
		{
			name:    "unsupported status only filters by status",
			include: []enums.ApplicationStatus{enums.ApplicationStatusOffer},
			expect: expectations{
				companies: []string{"Offer"},
			},
		},
		{
			name: "returns error when query fails",
			configureDB: func(db *gorm.DB) {
				db.Callback().Query().Before("gorm:query").Register("force_error", func(tx *gorm.DB) {
					tx.AddError(errors.New("boom"))
				})
			},
			expect: expectations{
				expectErr: true,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := testdb.NewSQLiteTestDB(t, &models.Salary{}, &models.Application{})
			insertApplications(t, db)

			if tt.configureDB != nil {
				tt.configureDB(db)
			}

			repo := repositories.NewApplicationRepository(newTestLogger(), db)

			result, err := repo.List(context.Background(), tt.include, tt.exclude, tt.isReply)

			if tt.expect.expectErr {
				require.Error(t, err)
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)

			var companies []string
			for _, app := range result {
				companies = append(companies, app.Company)
			}

			assert.ElementsMatch(t, tt.expect.companies, companies)
		})
	}
}

func TestApplicationRepositoryPaginate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	buildDB := func(t *testing.T) *gorm.DB {
		db := testdb.NewSQLiteTestDB(t, &models.Application{})

		for rowID := int64(1); rowID <= 5; rowID++ {
			app := newApplication(fmt.Sprintf("Company%d", rowID), enums.ApplicationStatusApplied, rowID, now)
			app.ID = rowID
			require.NoError(t, db.Create(&app).Error)
		}

		return db
	}

	tests := []struct {
		name        string
		params      dto.PaginationParams
		expectNext  *int64
		expectPrev  *int64
		expectLen   int
		expectError bool
		buildDB     func(t *testing.T) *gorm.DB
	}{
		{
			name:      "first page with next page",
			params:    dto.PaginationParams{Page: 1, PageSize: 2},
			expectLen: 2,
			expectNext: func() *int64 {
				next := int64(2)
				return &next
			}(),
		},
		{
			name:      "middle page has prev and next",
			params:    dto.PaginationParams{Page: 2, PageSize: 2},
			expectLen: 2,
			expectNext: func() *int64 {
				next := int64(3)
				return &next
			}(),
			expectPrev: func() *int64 {
				prev := int64(1)
				return &prev
			}(),
		},
		{
			name:      "last page has only prev",
			params:    dto.PaginationParams{Page: 3, PageSize: 2},
			expectLen: 1,
			expectPrev: func() *int64 {
				prev := int64(2)
				return &prev
			}(),
		},
		{
			name:        "returns error on query failure",
			params:      dto.PaginationParams{Page: 1, PageSize: 2},
			expectError: true,
		},
		{
			name:        "returns error when page fetch fails",
			params:      dto.PaginationParams{Page: 1, PageSize: 2},
			expectError: true,
			buildDB: func(t *testing.T) *gorm.DB {
				db := buildDB(t)
				var calls atomic.Int32
				db.Callback().Query().Before("gorm:query").Register("force_paginate_find_error", func(tx *gorm.DB) {
					if calls.Add(1) > 1 {
						tx.AddError(errors.New("force page error"))
					}
				})

				return db
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db *gorm.DB
			switch {
			case tt.buildDB != nil:
				db = tt.buildDB(t)
			case tt.expectError:
				db = testdb.NewSQLiteTestDB(t)
			default:
				db = buildDB(t)
			}

			repo := repositories.NewApplicationRepository(newTestLogger(), db)

			result, err := repo.Paginate(context.Background(), tt.params)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tt.params.Page, result.CurrentPage)
			assert.Equal(t, int64(3), result.LastPage)
			assert.Equal(t, tt.expectNext, result.NextPage)
			assert.Equal(t, tt.expectPrev, result.PreviousPage)
			assert.Len(t, result.Content, tt.expectLen)
		})
	}
}
