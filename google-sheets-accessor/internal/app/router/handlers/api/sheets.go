package api

import (
	"encoding/json"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router/handlers/api/messages"
)

type SheetsHandler struct {
	sheetsService sheetsService
}

func NewSheetsHandler(sheetsService sheetsService) *SheetsHandler {
	return &SheetsHandler{
		sheetsService: sheetsService,
	}
}

func (h *SheetsHandler) Get(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var sheetsGetRequest messages.SheetsGetRequest
	if err := decoder.Decode(&sheetsGetRequest); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)

		return
	}

	data, err := h.sheetsService.Get(
		r.Context(),
		sheetsGetRequest.SheetID,
		sheetsGetRequest.SheetPage,
		sheetsGetRequest.SheetCeilFrom,
		sheetsGetRequest.SheetCeilTo,
	)
	if err != nil {
		http.Error(w, "failed to get from sheet", http.StatusInternalServerError)

		return
	}

	resp := &messages.SheetsGetResponse{
		Data: data,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)

		return
	}
}
