package repositories

import (
	"context"
	"errors"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"gorm.io/gorm"
)

type ApplicationRepository struct {
	db *gorm.DB
}

func NewApplicationRepository(
	db *gorm.DB,
) *ApplicationRepository {
	return &ApplicationRepository{
		db: db,
	}
}

func (r *ApplicationRepository) FindByID(ctx context.Context, id int64) (*models.Application, error) {
	var application *models.Application
	if err := r.db.WithContext(ctx).Find(&application, id).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	return application, nil
}

func (r *ApplicationRepository) FindByRowID(ctx context.Context, rowID int64) (*models.Application, error) {
	var application *models.Application
	if err := r.db.WithContext(ctx).
		Preload("SalaryApplied").
		Preload("SalaryProposed").
		Where("row_id = ?", rowID).
		First(&application).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	return application, nil
}

func (r *ApplicationRepository) Save(ctx context.Context, application ...*models.Application) error {
	if err := r.db.WithContext(ctx).Save(&application).Error; err != nil {
		return err
	}

	return nil
}

func (r *ApplicationRepository) GetMaxRowID(ctx context.Context) (*int64, error) {
	var count *int64
	if err := r.db.WithContext(ctx).Model(&models.Application{}).Count(count).Error; err != nil {
		return nil, err
	}

	return count, nil
}
