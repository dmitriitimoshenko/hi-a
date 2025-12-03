package api

import (
	"encoding/json"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router/handlers/api/messages"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type ApplicationHandler struct {
	applicationService applicationService
}

func NewApplicationHandler(applicationService applicationService) *ApplicationHandler {
	return &ApplicationHandler{applicationService: applicationService}
}

func (h *ApplicationHandler) Diff(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var diffRequest messages.DiffRequest
	if err := decoder.Decode(&diffRequest); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)

		return
	}

	diffs, rowsChecked, err := h.applicationService.GetApplicationsDiff(
		r.Context(),
		diffRequest.SheetRange.StartRow,
		diffRequest.SheetRange.EndRow,
	)
	if err != nil {
		http.Error(w, "failed to get applications diff", http.StatusInternalServerError)

		return
	}

	mappedDiff, err := h.mapDiffToResponse(diffs)
	if err != nil {
		http.Error(w, "failed to map diff to response", http.StatusInternalServerError)

		return
	}

	resp := &messages.DiffResponse{
		Data: messages.DiffResponseData{
			RowsChecked:         *rowsChecked,
			RowsWithDifferences: int64(len(diffs)),
			Differences:         mappedDiff,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)

		return
	}
}

func (h *ApplicationHandler) mapDiffToResponse(diffs []dto.ApplicationDiffEntry) ([]messages.ApplicationDiffEntry, error) {
	var responseDiff []messages.ApplicationDiffEntry

	for _, applicationDiffs := range diffs {
		defferencies := []messages.ApplicationDiffChange{}
		for _, difference := range applicationDiffs.Differences {
			defferencies = append(defferencies, messages.ApplicationDiffChange{
				Field:      difference.Field,
				SheetValue: difference.SheetValue,
				DBValue:    difference.DBValue,
				Message:    difference.Message,
			})
		}
		responseDiff = append(responseDiff, messages.ApplicationDiffEntry{
			RowID:       applicationDiffs.RowID,
			Company:     applicationDiffs.Company,
			RoleTitle:   applicationDiffs.RoleTitle,
			Differences: defferencies,
			Errors:      applicationDiffs.Errors,
		})
	}

	return responseDiff, nil
}
