package api

import (
	"encoding/json"
	"net/http"
)

type HealthCheckHandler struct{}

type HealthCheckResponse struct {
	Status string `json:"status"`
	Msg    string `json:"msg,omitempty"`
}

func NewHealthCheckHandler() *HealthCheckHandler {
	return &HealthCheckHandler{}
}

func (h *HealthCheckHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := HealthCheckResponse{
		Status: "ok",
		Msg:    "Hire Event Processor service is healthy",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}
