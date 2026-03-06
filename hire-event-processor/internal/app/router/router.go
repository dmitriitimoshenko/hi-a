package router

import (
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/router/handlers/api"
)

func SetupRoutes(mux *http.ServeMux) {
	healthCheckHandler := api.NewHealthCheckHandler()

	mux.HandleFunc("GET /api/health-check", healthCheckHandler.HealthCheck)
}
