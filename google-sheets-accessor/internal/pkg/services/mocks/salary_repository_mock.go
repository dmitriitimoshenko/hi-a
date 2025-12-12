package mocks

import (
	"context"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/stretchr/testify/mock"
)

type SalaryRepositoryMock struct {
	mock.Mock
}

func (m *SalaryRepositoryMock) FindByID(ctx context.Context, id int64) (*models.Salary, error) {
	args := m.Called(ctx, id)

	salary, _ := args.Get(0).(*models.Salary)

	return salary, args.Error(1)
}

func (m *SalaryRepositoryMock) Save(ctx context.Context, salary ...*models.Salary) error {
	args := m.Called(ctx, salary)

	return args.Error(0)
}
