package repositories

import (
	"context"
	"errors"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"gorm.io/gorm"
)

type SalaryRepository struct {
	db *gorm.DB
}

func NewSalaryRepository(db *gorm.DB) *SalaryRepository {
	return &SalaryRepository{
		db: db,
	}
}

func (r *SalaryRepository) FindByID(ctx context.Context, id int64) (*models.Salary, error) {
	var salary *models.Salary
	if err := r.db.WithContext(ctx).Find(&salary, id).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	return salary, nil
}

func (r *SalaryRepository) Save(ctx context.Context, salary *models.Salary) error {
	if err := r.db.WithContext(ctx).Save(&salary).Error; err != nil {
		return err
	}

	return nil
}
