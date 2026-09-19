package handlers

import (
	"context"
	"errors"
	"log/slog"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
)

type ApplicationUpdateHandler struct {
	logger      *slog.Logger
	bus         publisher
	transformer *ApplicationUpdateTransformer
	topic       string
}

func NewApplicationUpdateHandler(
	logger *slog.Logger,
	busClient publisher,
	transformer *ApplicationUpdateTransformer,
	topic string,
) *ApplicationUpdateHandler {
	return &ApplicationUpdateHandler{
		logger:      logger,
		bus:         busClient,
		transformer: transformer,
		topic:       topic,
	}
}

func (h *ApplicationUpdateHandler) Handle(ctx context.Context, message busclient.Message) error {
	if h.topic == "" {
		return errors.New("STREAM_APPLICATION_UPDATE_PROCESSED is required")
	}

	payloadBytes, err := h.transformer.Transform(message.Value)
	if err != nil {
		h.logger.Error("failed to transform application-update payload", slog.Any("error", err))

		return err
	}

	err = h.bus.Publish(ctx, h.topic, message.Key, payloadBytes)
	if err != nil {
		h.logger.Error("failed to publish application-update message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("application-update message published", slog.String("topic", h.topic))

	return nil
}
