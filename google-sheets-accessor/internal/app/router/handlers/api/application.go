package api

import (
	"encoding/json"
	"net/http"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router/handlers/api/messages"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
)

type ApplicationHandler struct {
	applicationService applicationService
}

func NewApplicationHandler(applicationService applicationService) *ApplicationHandler {
	return &ApplicationHandler{applicationService: applicationService}
}

func (h *ApplicationHandler) Diff(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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

func (h *ApplicationHandler) Fetch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	applicationsSavedAmount, salariesSavedAmount, err := h.applicationService.Fetch(r.Context())
	if err != nil {
		http.Error(w, "failed to get applications fetch", http.StatusInternalServerError)

		return
	}

	resp := &messages.FetchRequest{
		Data: messages.FetchData{
			ApplicationsSavedAmount: applicationsSavedAmount,
			SalariesSavedAmount:     salariesSavedAmount,
		},
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)

		return
	}
}

func (h *ApplicationHandler) LastProcessedRow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	maxRowID, err := h.applicationService.GetMaxRowID(r.Context())
	if err != nil {
		http.Error(w, "failed to get Last Processed Row", http.StatusInternalServerError)

		return
	}
	if maxRowID == nil {
		http.Error(w, "failed to get Last Processed Row, it seem to be undefined", http.StatusInternalServerError)

		return
	}

	resp := &messages.LastProcessedRowResponse{
		Data: messages.LastProcessedRowData{
			LastProcessedRow: *maxRowID,
		},
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)

		return
	}
}

func (h *ApplicationHandler) List(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var diffRequest messages.ListRequest
	if err := decoder.Decode(&diffRequest); err != nil {
		http.Error(w, "invalid request payload", http.StatusBadRequest)

		return
	}

	list, err := h.applicationService.List(
		r.Context(),
		diffRequest.ApplicationStatusInclude,
		diffRequest.ApplicationStatusExclude,
		diffRequest.IsReplyEmailReceived,
	)
	if err != nil {
		http.Error(w, "failed to list applications", http.StatusInternalServerError)

		return
	}

	listResp := h.mapListToResponse(list)

	resp := &messages.ListResponse{
		Data: listResp,
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)

		return
	}
}

func (h *ApplicationHandler) mapListToResponse(applications []*models.Application) []messages.ListApplicationData {
	result := make([]messages.ListApplicationData, 0, len(applications))
	for _, application := range applications {
		meta := tools.FromJSONMap(application.Meta)
		item := messages.ListApplicationData{
			ID:                       application.ID,
			CreatedAt:                application.CreatedAt,
			UpdatedAt:                application.UpdatedAt,
			Company:                  application.Company,
			Title:                    application.Title,
			EmploymentType:           application.EmploymentType,
			WorkMode:                 application.WorkMode,
			Status:                   application.Status,
			AppliedAt:                application.AppliedAt,
			RespondedAt:              application.RespondedAt,
			NextFollowUpAt:           application.NextFollowUpAt,
			Stage:                    application.Stage,
			RowID:                    application.RowID,
			AppliedEmailReceived:     application.AppliedEmailReceived,
			AppliedEmailID:           application.AppliedEmailID,
			DeniedEmailReceived:      application.DeniedEmailReceived,
			DeniedEmailID:            application.DeniedEmailID,
			MeetingInvEmailReceived:  application.MeetingInvEmailReceived,
			MeetingInvEmailID:        application.MeetingInvEmailID,
			MeetingCrtEmailReceived:  application.MeetingCrtEmailReceived,
			MeetingCrtEmailID:        application.MeetingCrtEmailID,
			MeetingUpdEmailReceived:  application.MeetingUpdEmailReceived,
			MeetingUpdEmailID:        application.MeetingUpdEmailID,
			MeetingCnclEmailReceived: application.MeetingCnclEmailReceived,
			MeetingCnclEmailID:       application.MeetingCnclEmailID,
			Meta:                     &meta,
			Embedding:                application.Embedding.Slice(),
		}

		if application.SalaryApplied != nil {
			item.SalaryApplied = &messages.ListSalaryData{
				ID:         application.SalaryApplied.ID,
				CreatedAt:  application.SalaryApplied.CreatedAt,
				UpdatedAt:  application.SalaryApplied.UpdatedAt,
				AmountFrom: application.SalaryApplied.AmountFrom,
				AmountTo:   application.SalaryApplied.AmountTo,
				Currency:   application.SalaryApplied.Currency,
				Period:     application.SalaryApplied.Period,
			}
		}

		if application.SalaryProposed != nil {
			item.SalaryProposed = &messages.ListSalaryData{
				ID:         application.SalaryProposed.ID,
				CreatedAt:  application.SalaryProposed.CreatedAt,
				UpdatedAt:  application.SalaryProposed.UpdatedAt,
				AmountFrom: application.SalaryProposed.AmountFrom,
				AmountTo:   application.SalaryProposed.AmountTo,
				Currency:   application.SalaryProposed.Currency,
				Period:     application.SalaryProposed.Period,
			}
		}

		result = append(result, item)
	}

	return result
}
