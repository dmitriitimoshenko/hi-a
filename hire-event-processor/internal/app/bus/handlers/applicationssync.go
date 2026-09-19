package handlers

import (
	"context"
	"errors"
	"log/slog"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
)

type ApplicationSyncHandler struct {
	logger *slog.Logger
	bus    publisher
	topic  string
}

func NewApplicationSyncHandler(
	logger *slog.Logger,
	busClient publisher,
	topic string,
) *ApplicationSyncHandler {
	return &ApplicationSyncHandler{
		logger: logger,
		bus:    busClient,
		topic:  topic,
	}
}

func (h *ApplicationSyncHandler) Handle(ctx context.Context, message busclient.Message) error {
	if h.topic == "" {
		return errors.New("STREAM_APPLICATIONS_SYNC_PROCESSED is required")
	}

	err := h.bus.Publish(ctx, h.topic, message.Key, message.Value)
	if err != nil {
		h.logger.Error("failed to publish applications-sync message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("applications-sync message published", slog.String("topic", h.topic))

	return nil
}
