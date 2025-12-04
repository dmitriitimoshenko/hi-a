package router

import (
	"context"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router/handlers/api"
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
	FindByID(ctx context.Context, id int64) (*models.Application, error)
	SyncFromDTO(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error
	Update(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error
	SyncFromDB(ctx context.Context, id int64) error
	SyncFromSheet(ctx context.Context, rowID int64) error
	CleanUpMeetingsInBatches(ctx context.Context, batchSize int64) (int64, error)
}

func SetupRoutes(
	mux *http.ServeMux,
	applicationService applicationService,
) {
	healthCheckHandler := api.NewHealthCheckHandler()
	applicationHandler := api.NewApplicationHandler(applicationService)

	mux.HandleFunc("GET /api/health-check", healthCheckHandler.HealthCheck)

	mux.HandleFunc("POST /api/application/diff", applicationHandler.Diff)
	mux.HandleFunc("POST /api/application/fetch", applicationHandler.Fetch)
	mux.HandleFunc("POST /api/application/last-processed-row", applicationHandler.LastProcessedRow)
	mux.HandleFunc("POST /api/application/list", applicationHandler.List)
	mux.HandleFunc("POST /api/application/diff/update", applicationHandler.Update)
	mux.HandleFunc("POST /api/application/cleanup-meetings", applicationHandler.CleanUpMeetings)

	// mux.HandleFunc("POST /api/sheet/get", sheetHandler.Get)
}
