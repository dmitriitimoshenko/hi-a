package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
)

type EmbeddingPayload []float32

type SaveApplicationEmbeddingHandler struct {
	applicationService applicationService
	logger             *slog.Logger
}

func NewSaveApplicationEmbeddingHandler(
	applicationService applicationService,
	logger *slog.Logger,
) *SaveApplicationEmbeddingHandler {
	return &SaveApplicationEmbeddingHandler{
		applicationService: applicationService,
		logger:             logger,
	}
}

func (h *SaveApplicationEmbeddingHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	var embedding EmbeddingPayload
	if err := json.Unmarshal(message.Value, &embedding); err != nil {
		return err
	}

	h.logger.Info(
		"kafka key on SaveApplicationEmbeddingHandler will be parsed",
		slog.String("messageKey", string(message.Key)),
	)

	mk := binary.BigEndian.Uint64(message.Key)
	applicationID := int64(mk)
	h.logger.Info(
		"kafka key on SaveApplicationEmbeddingHandler will was parsed",
		slog.Int64("applicationID", applicationID),
	)

	if err := h.applicationService.AddEmbeddingByID(ctx, applicationID, embedding); err != nil {
		return err
	}

	return nil
}
