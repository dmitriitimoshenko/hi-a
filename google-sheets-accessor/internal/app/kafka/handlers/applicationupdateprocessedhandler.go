package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	aup "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka/handlers/messages/applicationupdateprocessed"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type ApplicationUpdateProcessedHandler struct {
	applicationService applicationService
}

func NewApplicationUpdateProcessedHandler(applicationService applicationService) *ApplicationUpdateProcessedHandler {
	return &ApplicationUpdateProcessedHandler{
		applicationService: applicationService,
	}
}

func (h *ApplicationUpdateProcessedHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	var applicationUpdateData *aup.ApplicationUpdatePayload
	if err := json.Unmarshal(message.Value, &applicationUpdateData); err != nil {
		return err
	}

	key := string(message.Key)

	if applicationUpdateData == nil {
		return errors.New("failed to extract applicationUpdateData from kafka message")
	}

	mappedApplication := applicationUpdateData.MappedApplication
	mappedApplicationSalaryApplied := applicationUpdateData.MappedApplication.SalaryApplied
	mappedApplicationSalaryProposed := applicationUpdateData.MappedApplication.SalaryProposed

	updateApplicationSalaryAppliedDTO := dto.UpdateApplicationSalaryDTO{
		ID:         mappedApplicationSalaryApplied.ID,
		AmountFrom: mappedApplicationSalaryApplied.AmountFrom,
		AmountTo:   mappedApplicationSalaryApplied.AmountTo,
		Currency:   mappedApplicationSalaryApplied.Currency,
		Period:     mappedApplicationSalaryApplied.Period,
	}
	updateApplicationSalaryProposedDTO := dto.UpdateApplicationSalaryDTO{
		ID:         mappedApplicationSalaryProposed.ID,
		AmountFrom: mappedApplicationSalaryProposed.AmountFrom,
		AmountTo:   mappedApplicationSalaryProposed.AmountTo,
		Currency:   mappedApplicationSalaryProposed.Currency,
		Period:     mappedApplicationSalaryProposed.Period,
	}
	updateApplicationDTO := dto.UpdateApplicationDTO{
		ID:             mappedApplication.ID,
		RowID:          mappedApplication.RowID,
		Company:        mappedApplication.Company,
		Title:          mappedApplication.Title,
		EmploymentType: mappedApplication.EmploymentType,
		WorkMode:       mappedApplication.WorkMode,
		Status:         mappedApplication.Status,
		AppliedAt:      mappedApplication.AppliedAt,
		RespondedAt:    mappedApplication.RespondedAt,
		NextFollowUpAt: mappedApplication.NextFollowUpAt,
		Stage:          mappedApplication.Stage,
		Meta:           mappedApplication.Meta,
		Embedding:      mappedApplication.Embedding,
		SalaryApplied:  &updateApplicationSalaryAppliedDTO,
		SalaryProposed: &updateApplicationSalaryProposedDTO,
	}

	if err := h.applicationService.UpdateAndSync(ctx, updateApplicationDTO); err != nil {
		return fmt.Errorf("failed to apply application update with kafka key [%s]", key)
	}

	return nil
}
