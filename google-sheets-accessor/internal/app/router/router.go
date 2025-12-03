package router

import (
	"context"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router/handlers/api"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error)
}

func SetupRoutes(
	mux *http.ServeMux,
	applicationService applicationService,
) {
	healthCheckHandler := api.NewHealthCheckHandler()
	applicationHandler := api.NewApplicationHandler(applicationService)

	mux.HandleFunc("GET /api/health-check", healthCheckHandler.HealthCheck)

	mux.HandleFunc("POST /api/application/diff", applicationHandler.Diff)
	// mux.HandleFunc("POST /api/application/fetch", applicationHandler.Fetch)
	// mux.HandleFunc("POST /api/application/list", applicationHandler.List)
	// mux.HandleFunc("POST /api/application/last-processed-row", applicationHandler.LastProcessedRow)
	// mux.HandleFunc("POST /api/application/update-internal", applicationHandler.UpdateInternal)
	// mux.HandleFunc("POST /api/application/update-external", applicationHandler.UpdateExternal)
	// mux.HandleFunc("POST /api/application/cleanup-meetings", applicationHandler.CleanUpMeetings)

	// mux.HandleFunc("POST /api/sheet/get", sheetHandler.Get)
}
