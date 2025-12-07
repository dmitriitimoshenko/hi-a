package handlers

import (
	"context"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
)

type NotificationSyncHandler struct {
	logger *slog.Logger
}

func NewNotificationSyncHandler(logger *slog.Logger) *NotificationSyncHandler {
	return &NotificationSyncHandler{
		logger: logger,
	}
}

func (h *NotificationSyncHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling notification sync message", "message", message)

	// Implement your message handling logic here

	return nil
}
