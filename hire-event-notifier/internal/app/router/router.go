package router

import (
	"log/slog"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/router/handlers/api"
)

func SetupRoutes(
	mux *http.ServeMux,
	logger *slog.Logger,
) {
	healthCheckHandler := api.NewHealthCheckHandler()

	mux.HandleFunc("GET /api/health-check", healthCheckHandler.HealthCheck)
}
