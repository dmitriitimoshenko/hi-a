package handlers

import (
	"context"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
)

type NotificationHandler struct {
	logger *slog.Logger
}

func NewNotificationHandler(logger *slog.Logger) *NotificationHandler {
	return &NotificationHandler{
		logger: logger,
	}
}

func (h *NotificationHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling notification message", "message", message)

	// Implement your message handling logic here

	return nil
}
