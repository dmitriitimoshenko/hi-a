package handlers

import (
	"context"
	"errors"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
)

type ApplicationUpdateHandler struct {
	logger      *slog.Logger
	kafka       publisher
	transformer *ApplicationUpdateTransformer
	topic       string
}

func NewApplicationUpdateHandler(
	logger *slog.Logger,
	kafkaClient publisher,
	transformer *ApplicationUpdateTransformer,
	topic string,
) *ApplicationUpdateHandler {
	return &ApplicationUpdateHandler{
		logger:      logger,
		kafka:       kafkaClient,
		transformer: transformer,
		topic:       topic,
	}
}

func (h *ApplicationUpdateHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	if h.topic == "" {
		return errors.New("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED is required")
	}

	payloadBytes, err := h.transformer.Transform(message.Value)
	if err != nil {
		h.logger.Error("failed to transform application-update payload", slog.Any("error", err))

		return err
	}

	err = h.kafka.Publish(ctx, h.topic, message.Key, payloadBytes)
	if err != nil {
		h.logger.Error("failed to publish application-update message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("application-update message published", slog.String("topic", h.topic))

	return nil
}
