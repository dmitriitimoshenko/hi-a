package handlers

import (
	"context"
	"errors"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
)

type InterestingMailHandler struct {
	logger *slog.Logger
	kafka  publisher
	topic  string
}

func NewInterestingMailHandler(
	logger *slog.Logger,
	kafkaClient publisher,
	topic string,
) *InterestingMailHandler {
	return &InterestingMailHandler{
		logger: logger,
		kafka:  kafkaClient,
		topic:  topic,
	}
}

func (h *InterestingMailHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	if h.topic == "" {
		return errors.New("KAFKA_TOPIC_HIRE_EVENT is required")
	}

	err := h.kafka.Publish(ctx, h.topic, message.Key, message.Value)
	if err != nil {
		h.logger.Error("failed to publish hire-event message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("hire-event message published", slog.String("topic", h.topic))

	return nil
}
