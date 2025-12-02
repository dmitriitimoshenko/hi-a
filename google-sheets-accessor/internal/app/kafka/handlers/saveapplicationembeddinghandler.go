package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
)

type EmbeddingPayload []float32

type SaveApplicationEmbeddingHandler struct {
	applicationService applicationService
}

func NewSaveApplicationEmbeddingHandler(applicationService applicationService) *SaveApplicationEmbeddingHandler {
	return &SaveApplicationEmbeddingHandler{
		applicationService: applicationService,
	}
}

func (h *SaveApplicationEmbeddingHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	var embedding EmbeddingPayload
	if err := json.Unmarshal(message.Value, &embedding); err != nil {
		return err
	}

	applicationID := int64(binary.BigEndian.Uint64(message.Key))

	if err := h.applicationService.AddEmbeddingByID(ctx, applicationID, embedding); err != nil {
		return err
	}

	return nil
}
