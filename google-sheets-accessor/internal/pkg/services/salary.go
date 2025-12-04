package services

import (
	"context"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
)

type SalaryService struct {
	repository salaryRepository
}

func NewSalaryService(reposotory salaryRepository) *SalaryService {
	return &SalaryService{
		repository: reposotory,
	}
}

func (s *SalaryService) FindByID(ctx context.Context, id int64) (*models.Salary, error) {
	salary, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return salary, nil
}

func (s *SalaryService) Save(ctx context.Context, salary ...*models.Salary) error {
	if err := s.repository.Save(ctx, salary...); err != nil {
		return err
	}

	return nil
}
