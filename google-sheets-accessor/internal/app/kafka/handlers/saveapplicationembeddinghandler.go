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

	mk := string(message.Key)
	applicationID, err := strconv.Atoi(mk)
	if err != nil {
		return err
	}

	if err := h.applicationService.AddEmbeddingByID(ctx, int64(applicationID), embedding); err != nil {
		return err
	}

	return nil
}
