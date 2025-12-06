package services

import (
	"context"
	"errors"
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/mocks"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewSalaryService(t *testing.T) {
	repo := &mocks.SalaryRepositoryMock{}

	service := NewSalaryService(repo)

	assert.Equal(t, service.repository, repo)
}

func TestSalaryServiceFindByIDSuccess(t *testing.T) {
	expectedSalary := &models.Salary{ID: 10, Currency: "USD"}
	repo := &mocks.SalaryRepositoryMock{}
	repo.On("FindByID", mock.Anything, int64(10)).Return(expectedSalary, nil).Once()

	service := NewSalaryService(repo)

	result, err := service.FindByID(context.Background(), 10)

	assert.Equal(t, err, nil)
	assert.Equal(t, expectedSalary, result)
	repo.AssertExpectations(t)
}

func TestSalaryServiceFindByIDError(t *testing.T) {
	expectedErr := errors.New("find failed")
	repo := &mocks.SalaryRepositoryMock{}
	repo.On("FindByID", mock.Anything, int64(20)).Return((*models.Salary)(nil), expectedErr).Once()

	service := NewSalaryService(repo)

	result, err := service.FindByID(context.Background(), 20)

	assert.Equal(t, err, expectedErr)
	assert.Equal(t, result, (*models.Salary)(nil))
	repo.AssertExpectations(t)
}

func TestSalaryServiceSaveSuccess(t *testing.T) {
	repo := &mocks.SalaryRepositoryMock{}
	s1 := &models.Salary{ID: 1}
	s2 := &models.Salary{ID: 2}

	repo.On("Save", mock.Anything, mock.MatchedBy(func(v interface{}) bool {
		salaries, ok := v.([]*models.Salary)
		if !ok {
			return false
		}

		return len(salaries) == 2 && salaries[0] == s1 && salaries[1] == s2
	})).Return(nil).Once()

	service := NewSalaryService(repo)

	err := service.Save(context.Background(), s1, s2)

	assert.Equal(t, err, nil)
	repo.AssertExpectations(t)
}

func TestSalaryServiceSaveError(t *testing.T) {
	expectedErr := errors.New("save failed")
	repo := &mocks.SalaryRepositoryMock{}

	repo.On("Save", mock.Anything, mock.Anything).Return(expectedErr).Once()

	service := NewSalaryService(repo)

	err := service.Save(context.Background(), &models.Salary{ID: 3})

	assert.Equal(t, err, expectedErr)
	repo.AssertExpectations(t)
}
