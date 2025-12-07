package repositories_test

import (
	"context"
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/repositories"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools/testhelper/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestSalaryRepositoryFindByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(t *testing.T) *repositories.SalaryRepository
		expectFound bool
		expectError bool
	}{
		{
			name: "found",
			setup: func(t *testing.T) *repositories.SalaryRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{})
				salary := models.Salary{Currency: "USD", Period: enums.SalaryPeriodMonthly}
				require.NoError(t, db.Create(&salary).Error)

				return repositories.NewSalaryRepository(db)
			},
			expectFound: true,
		},
		{
			name: "not found returns nil",
			setup: func(t *testing.T) *repositories.SalaryRepository {
				db := testdb.NewSQLiteTestDB(t, &models.Salary{})

				return repositories.NewSalaryRepository(db)
			},
		},
		{
			name: "query error bubbles up",
			setup: func(t *testing.T) *repositories.SalaryRepository {
				db := testdb.NewSQLiteTestDB(t)

				return repositories.NewSalaryRepository(db)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := tt.setup(t)

			result, err := repo.FindByID(context.Background(), 1)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)
			if tt.expectFound {
				require.NotNil(t, result)
				assert.Equal(t, "USD", result.Currency)
				assert.Equal(t, enums.SalaryPeriodMonthly, result.Period)
			} else if result != nil {
				assert.Zero(t, result.ID)
			}
		})
	}
}

func TestSalaryRepositorySave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		db          *gorm.DB
		salary      *models.Salary
		expectError bool
	}{
		{
			name: "persists salary",
			db:   testdb.NewSQLiteTestDB(t, &models.Salary{}),
			salary: &models.Salary{
				Currency: "EUR",
				Period:   enums.SalaryPeriodYearly,
			},
		},
		{
			name: "returns error on failure",
			db:   testdb.NewSQLiteTestDB(t),
			salary: &models.Salary{
				Currency: "CHF",
				Period:   enums.SalaryPeriodMonthly,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := repositories.NewSalaryRepository(tt.db)

			err := repo.Save(context.Background(), tt.salary)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			var stored models.Salary
			require.NoError(t, tt.db.First(&stored).Error)
			assert.Equal(t, tt.salary.Currency, stored.Currency)
			assert.Equal(t, tt.salary.Period, stored.Period)
		})
	}
}
