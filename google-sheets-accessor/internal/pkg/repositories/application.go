package repositories

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"gorm.io/gorm"
)

type ApplicationRepository struct {
	db     *gorm.DB
	logger *slog.Logger
}

func NewApplicationRepository(
	db *gorm.DB,
	logger *slog.Logger,
) *ApplicationRepository {
	return &ApplicationRepository{
		db:     db,
		logger: logger,
	}
}

func (r *ApplicationRepository) FindByID(ctx context.Context, id int64) (*models.Application, error) {
	var application *models.Application
	if err := r.db.WithContext(ctx).
		Preload("SalaryApplied").
		Preload("SalaryProposed").
		Find(&application, id).Error; err != nil {
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
	var count int64
	if err := r.db.WithContext(ctx).Model(&models.Application{}).Count(&count).Error; err != nil {
		return nil, err
	}

	return &count, nil
}

func (r *ApplicationRepository) List(
	ctx context.Context,
	applicationStatusInclude []enums.ApplicationStatus,
	ApplicationStatusExclude []enums.ApplicationStatus,
	IsReplyEmailReceived bool,
) ([]*models.Application, error) {
	query := r.db.WithContext(ctx).
		Model(&models.Application{}).
		Preload("SalaryApplied").
		Preload("SalaryProposed")

	if len(ApplicationStatusExclude) > 0 {
		query = query.Where("status NOT IN ?", ApplicationStatusExclude)
	} else if len(applicationStatusInclude) > 0 {
		query = query.Where("status IN ?", applicationStatusInclude)

		status := applicationStatusInclude[0]
		switch status {
		case enums.ApplicationStatusApplied:
			if IsReplyEmailReceived {
				query = query.Where("applied_email_received IS NOT NULL AND applied_email_id IS NOT NULL")
			} else {
				query = query.Where("NOT (applied_email_received IS NOT NULL AND applied_email_id IS NOT NULL)")
			}
		case enums.ApplicationStatusDenied:
			if IsReplyEmailReceived {
				query = query.Where("denied_email_received IS NOT NULL AND denied_email_id IS NOT NULL")
			} else {
				query = query.Where("NOT (denied_email_received IS NOT NULL AND denied_email_id IS NOT NULL)")
			}
		default:
			r.logger.Info(
				fmt.Sprintf("Filtering by is_reply_email_received is not supported for status: %v", applicationStatusInclude),
			)
		}
	}

	var applications []*models.Application
	if err := query.Find(&applications).Error; err != nil {
		return nil, fmt.Errorf("failed to fetch newest applications: %w", err)
	}

	return applications, nil
}

func (r *ApplicationRepository) Paginate(ctx context.Context, pp dto.PaginationParams) (*dto.PaginatedApplications, error) {
	var (
		appliations                []*models.Application
		nextPage, previousPage     *int64
		appliationsCount, lastPage int64
	)

	if err := r.db.WithContext(ctx).
		Model(&models.Application{}).
		Offset(int(pp.PageSize * (pp.Page - 1))).
		Limit(int(pp.PageSize)).
		Find(&appliations).
		Count(&appliationsCount).
		Error; err != nil {
		return nil, err
	}

	lastPage = appliationsCount / pp.PageSize
	if appliationsCount%pp.PageSize != 0 {
		lastPage++
	}

	if lastPage > pp.Page {
		nextPage = tools.ToPtr(pp.Page + 1)
	}

	if 1 < pp.Page {
		previousPage = tools.ToPtr(pp.Page - 1)
	}

	return &dto.PaginatedApplications{
		CurrentPage:  pp.Page,
		LastPage:     lastPage,
		NextPage:     nextPage,
		PreviousPage: previousPage,
		Content:      appliations,
	}, nil
}
