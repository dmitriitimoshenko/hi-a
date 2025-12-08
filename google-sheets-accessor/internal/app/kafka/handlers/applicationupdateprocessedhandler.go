package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	aup "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka/handlers/messages/applicationupdateprocessed"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
)

type ApplicationUpdateProcessedHandler struct {
	logger             *slog.Logger
	applicationService applicationService
}

func NewApplicationUpdateProcessedHandler(
	logger *slog.Logger,
	applicationService applicationService,
) *ApplicationUpdateProcessedHandler {
	return &ApplicationUpdateProcessedHandler{
		logger:             logger,
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
	mappedApplicationSalaryApplied := mappedApplication.SalaryApplied
	mappedApplicationSalaryProposed := mappedApplication.SalaryProposed

	meta, err := tools.ByteToMapStringString(mappedApplication.Meta)
	if err != nil {
		return fmt.Errorf("failed to ByteToMapStringString: %w", err)
	}

	var updateApplicationSalaryAppliedDTO, updateApplicationSalaryProposedDTO *dto.UpdateApplicationSalaryDTO
	if mappedApplicationSalaryApplied != nil {
		updateApplicationSalaryAppliedDTO = &dto.UpdateApplicationSalaryDTO{
			ID:         mappedApplicationSalaryApplied.ID,
			AmountFrom: mappedApplicationSalaryApplied.AmountFrom,
			AmountTo:   mappedApplicationSalaryApplied.AmountTo,
			Currency:   mappedApplicationSalaryApplied.Currency,
			Period:     mappedApplicationSalaryApplied.Period,
		}
	}
	if mappedApplicationSalaryProposed != nil {
		updateApplicationSalaryProposedDTO = &dto.UpdateApplicationSalaryDTO{
			ID:         mappedApplicationSalaryProposed.ID,
			AmountFrom: mappedApplicationSalaryProposed.AmountFrom,
			AmountTo:   mappedApplicationSalaryProposed.AmountTo,
			Currency:   mappedApplicationSalaryProposed.Currency,
			Period:     mappedApplicationSalaryProposed.Period,
		}
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
		Meta:           *meta,
		SalaryApplied:  updateApplicationSalaryAppliedDTO,
		SalaryProposed: updateApplicationSalaryProposedDTO,
	}

	if err := h.applicationService.UpdateAndSync(ctx, updateApplicationDTO); err != nil {
		h.logger.Error(
			"failed to apply application update",
			slog.String("kafka_key", key),
			slog.Any("application_update_data", applicationUpdateData),
			slog.String("err", err.Error()),
		)

		return fmt.Errorf("failed to apply application update with kafka key [%s]", key)
	}

	return nil
}
