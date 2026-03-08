package handlers

import (
	"context"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app/kafka"
)

type CanendarEventsHandler struct {
	calendarService calendarService
	logger          *slog.Logger
}

func NewCanendarEventsHandler(calendarService calendarService, logger *slog.Logger) *CanendarEventsHandler {
	return &CanendarEventsHandler{
		calendarService: calendarService,
		logger:          logger,
	}
}

func (h *CanendarEventsHandler) ProcessEvent(ctx context.Context, message kafkaclient.Message) error {
	return nil
}
