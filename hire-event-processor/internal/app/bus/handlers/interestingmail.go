package handlers

import (
	"context"
	"errors"
	"log/slog"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
)

type InterestingMailHandler struct {
	logger *slog.Logger
	bus    publisher
	topic  string
}

func NewInterestingMailHandler(
	logger *slog.Logger,
	busClient publisher,
	topic string,
) *InterestingMailHandler {
	return &InterestingMailHandler{
		logger: logger,
		bus:    busClient,
		topic:  topic,
	}
}

func (h *InterestingMailHandler) Handle(ctx context.Context, message busclient.Message) error {
	if h.topic == "" {
		return errors.New("STREAM_HIRE_EVENT is required")
	}

	err := h.bus.Publish(ctx, h.topic, message.Key, message.Value)
	if err != nil {
		h.logger.Error("failed to publish hire-event message", slog.String("topic", h.topic), slog.Any("error", err))

		return err
	}

	h.logger.Info("hire-event message published", slog.String("topic", h.topic))

	return nil
}
