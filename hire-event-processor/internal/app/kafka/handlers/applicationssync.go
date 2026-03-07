package handlers

import (
	"context"
	"errors"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
)

type ApplicationSyncHandler struct {
	logger *slog.Logger
	kafka  publisher
	topic  string
}

func NewApplicationSyncHandler(
	logger *slog.Logger,
	kafkaClient publisher,
	topic string,
) *ApplicationSyncHandler {
	return &ApplicationSyncHandler{
		logger: logger,
		kafka:  kafkaClient,
		topic:  topic,
	}
}

func (h *ApplicationSyncHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	if h.topic == "" {
		return errors.New("KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED is required")
	}

	err := h.kafka.Publish(ctx, h.topic, message.Key, message.Value)
	if err != nil {
		h.logger.Error("failed to publish applications-sync message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("applications-sync message published", slog.String("topic", h.topic))

	return nil
}
