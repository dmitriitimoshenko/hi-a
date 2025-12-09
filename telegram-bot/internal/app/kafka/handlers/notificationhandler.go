package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka/handlers/messages"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
)

type NotificationHandler struct {
	logger      *slog.Logger
	redisClient redisClient
	tgbtService tgbtService
}

func NewNotificationHandler(
	logger *slog.Logger,
	redisClient redisClient,
	tgbtService tgbtService,
) *NotificationHandler {
	return &NotificationHandler{
		logger:      logger,
		redisClient: redisClient,
		tgbtService: tgbtService,
	}
}

func (h *NotificationHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling notification message", "message", message)

	var notificationMessage *messages.NotificationMessage
	if err := json.Unmarshal(message.Value, &notificationMessage); err != nil {
		h.logger.Error("failed to unmarshal notification message", "err", err)
		return err
	}

	newEmailMappedMessage := h.tgbtService.BuildNewMappedEmailMessage(
		dto.NewMappedEmailMessageContent{
			Label:             notificationMessage.Email.Label,
			Email:             notificationMessage.Email,
			MappedApplication: notificationMessage.MappedApplication,
			EmailID:           notificationMessage.Email.ID,
		},
	)

	newEmailMappedKeyboard := h.tgbtService.BuildNewMappedEmailKeyboard(
		notificationMessage.Email.Label,
		notificationMessage.Email.ID,
		false,
		false,
	)

	cacheKey := string(message.Key)
	ok, err := h.redisClient.Set(ctx, cacheKey, string(message.Value), applicationDiffCacheTTL)
	if err != nil {
		h.logger.Error("failed to set cache for application diff", "err", err)
		return err
	}
	if !ok {
		h.logger.Error("cache set operation returned false", "key", cacheKey)
		return fmt.Errorf("failed to set cache for key %s", cacheKey)
	}

	if err = h.tgbtService.SendMessage(ctx, newEmailMappedMessage, newEmailMappedKeyboard); err != nil {
		h.logger.Error("failed to send new mapped email message", "err", err)
		return err
	}

	return nil
}
