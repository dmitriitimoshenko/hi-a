package api

import (
	"context"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error)
	Fetch(ctx context.Context) (int64, int64, error)
	GetMaxRowID(ctx context.Context) (*int64, error)
	List(
		ctx context.Context,
		applicationStatusInclude []enums.ApplicationStatus,
		applicationStatusExclude []enums.ApplicationStatus,
		isReplyEmailReceived bool,
	) ([]*models.Application, error)
	SyncFromDTO(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error
	FindByID(ctx context.Context, id int64) (*models.Application, error)
	Update(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error
	SyncFromDB(ctx context.Context, id int64) error
	SyncFromSheet(ctx context.Context, rowID int64) error
	CleanUpMeetingsInBatches(ctx context.Context, batchSize int64) (int64, error)
}

type sheetsService interface {
	Get(ctx context.Context, sheetID, sheetPage, ceilFrom, ceilTo string) ([][]string, error)
}
