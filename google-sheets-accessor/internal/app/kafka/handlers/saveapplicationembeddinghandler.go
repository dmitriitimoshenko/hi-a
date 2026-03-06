package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
)

type EmbeddingPayload []float32

type SaveApplicationEmbeddingHandler struct {
	logger             *slog.Logger
	applicationService applicationService
}

func NewSaveApplicationEmbeddingHandler(
	logger *slog.Logger,
	applicationService applicationService,
) *SaveApplicationEmbeddingHandler {
	return &SaveApplicationEmbeddingHandler{
		logger:             logger,
		applicationService: applicationService,
	}
}

func (h *SaveApplicationEmbeddingHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	var embedding EmbeddingPayload
	if err := json.Unmarshal(message.Value, &embedding); err != nil {
		h.logger.Error("[SaveApplicationEmbeddingHandler] failed to unmarshal value from Kafka message", "err", err, slog.Any("message.Value", message.Value))
		return err
	}

	mk := string(message.Key)
	applicationID, err := strconv.Atoi(mk)
	if err != nil {
		h.logger.Error("[SaveApplicationEmbeddingHandler] failed to convert application ID from Kafka message key", "err", err, slog.String("message.Key", mk))
		return err
	}

	if err := h.applicationService.AddEmbeddingByID(ctx, int64(applicationID), embedding); err != nil {
		h.logger.Error("[SaveApplicationEmbeddingHandler] failed to add embedding to application", "err", err, slog.Int("application_id", applicationID))
		return err
	}

	return nil
}
