package handlers

import (
	"context"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
)

type ApplicationUpdateProcessedHandler struct {
	logger *slog.Logger
}

func NewApplicationUpdateProcessedHandler(logger *slog.Logger) *ApplicationUpdateProcessedHandler {
	return &ApplicationUpdateProcessedHandler{
		logger: logger,
	}
}

func (h *ApplicationUpdateProcessedHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	return nil
}
