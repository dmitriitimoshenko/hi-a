package services_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/mocks"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/mock"
)

func TestSalaryServiceFindByID(t *testing.T) {
	salary := &models.Salary{ID: 10, Currency: "USD"}
	repositoryErr := errors.New("find failed")
	resultWithError := errors.New("find returned object with error")

	tests := []struct {
		name           string
		id             int64
		returnedSalary *models.Salary
		returnedErr    error
		expectedSalary *models.Salary
		expectedErr    error
	}{
		{
			name:           "success",
			id:             10,
			returnedSalary: salary,
			expectedSalary: salary,
		},
		{
			name:        "repository error",
			id:          20,
			returnedErr: repositoryErr,
			expectedErr: repositoryErr,
		},
		{
			name: "nil result no error",
			id:   30,
		},
		{
			name:           "result with error",
			id:             40,
			returnedSalary: &models.Salary{ID: 40},
			returnedErr:    resultWithError,
			expectedErr:    resultWithError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.SalaryRepositoryMock{}
			repo.On("FindByID", mock.Anything, tt.id).Return(tt.returnedSalary, tt.returnedErr).Once()

			service := services.NewSalaryService(repo)

			result, err := service.FindByID(context.Background(), tt.id)

			assert.Equal(t, tt.expectedErr, err)
			assert.Equal(t, tt.expectedSalary, result)
			repo.AssertExpectations(t)
		})
	}
}

func TestSalaryServiceSave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		salaries  []*models.Salary
		setupRepo func(repo *mocks.SalaryRepositoryMock)
		expected  error
	}{
		{
			name: "success",
			salaries: []*models.Salary{
				{ID: 1},
				{ID: 2},
			},
			setupRepo: func(repo *mocks.SalaryRepositoryMock) {
				repo.On("Save", mock.Anything, mock.MatchedBy(func(v interface{}) bool {
					salaries, ok := v.([]*models.Salary)
					if !ok {
						return false
					}

					return len(salaries) == 2 && salaries[0].ID == 1 && salaries[1].ID == 2
				})).Return(nil).Once()
			},
		},
		{
			name: "error",
			salaries: []*models.Salary{
				{ID: 3},
			},
			setupRepo: func(repo *mocks.SalaryRepositoryMock) {
				repo.On("Save", mock.Anything, mock.Anything).Return(errors.New("save failed")).Once()
			},
			expected: errors.New("save failed"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &mocks.SalaryRepositoryMock{}
			tt.setupRepo(repo)

			service := services.NewSalaryService(repo)

			err := service.Save(context.Background(), tt.salaries...)

			assert.Equal(t, tt.expected, err)
			repo.AssertExpectations(t)
		})
	}
}
