package handlers

import (
	"context"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
)

type HireEventHandler struct {
	logger *slog.Logger
}

func NewHireEventHandler(logger *slog.Logger) *HireEventHandler {
	return &HireEventHandler{
		logger: logger,
	}
}

func (h *HireEventHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	return nil
}
