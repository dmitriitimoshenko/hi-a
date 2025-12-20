package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka/handlers/messages"
)

type HireEventHandler struct {
	logger *slog.Logger
	kafka  *kafkaclient.Client
}

func NewHireEventHandler(logger *slog.Logger, kafkaClient *kafkaclient.Client) *HireEventHandler {
	return &HireEventHandler{
		logger: logger,
		kafka:  kafkaClient,
	}
}

func (h *HireEventHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling hire-event message")

	var hireEvent messages.HireEventMessage
	if err := json.Unmarshal(message.Value, &hireEvent); err != nil {
		h.logger.Error("failed to unmarshal hire-event message", slog.Any("error", err))

		return err
	}

	hireEvent.MappedApplication.Embedding = nil

	payloadBytes, err := json.Marshal(hireEvent)
	if err != nil {
		h.logger.Error("failed to marshal notification payload", slog.Any("error", err))

		return err
	}

	notificationTopic := os.Getenv("KAFKA_TOPIC_NOTIFICATION")
	if notificationTopic == "" {
		return errors.New("KAFKA_TOPIC_NOTIFICATION is required")
	}

	if err := h.kafka.Publish(ctx, notificationTopic, message.Key, payloadBytes); err != nil {
		h.logger.Error(
			"failed to publish hire-event notification",
			slog.String("topic", notificationTopic),
			slog.Any("error", err),
		)

		return err
	}

	h.logger.Info(
		"hire-event notification published",
		slog.String("topic", notificationTopic),
	)

	return nil
}
