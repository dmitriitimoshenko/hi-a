package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka/handlers/messages"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
)

const applicationDiffCacheTTL = 31 * 24 * time.Hour

type NotificationSyncHandler struct {
	logger      *slog.Logger
	redisClient redisClient
	tgbtService tgbtService
}

func NewNotificationSyncHandler(logger *slog.Logger, redisClient redisClient, tgbtService tgbtService) *NotificationSyncHandler {
	return &NotificationSyncHandler{
		logger:      logger,
		redisClient: redisClient,
		tgbtService: tgbtService,
	}
}

func (h *NotificationSyncHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling notification sync message", "message", message)

	var notificationSyncMessage *messages.NotificationSyncMessage
	if err := json.Unmarshal(message.Value, &notificationSyncMessage); err != nil {
		h.logger.Error("failed to unmarshal notification sync message", "err", err)
		return err
	}

	var detectedAt time.Time
	if notificationSyncMessage.DetectedAt == "" {
		h.logger.Warn("[NotificationSyncHandler] detectedAt time is empty")
	} else {
		var err error
		detectedAt, err = time.Parse(time.RFC3339, notificationSyncMessage.DetectedAt)
		if err != nil {
			h.logger.Error("failed to parse detected at time", "err", err)
			return err
		}
	}

	diffMessage := h.tgbtService.BuildApplicationDiffMessage(
		dto.DiffMessageContent{
			Company:      notificationSyncMessage.Company,
			Role:         notificationSyncMessage.RoleTitle,
			Differencies: notificationSyncMessage.Differences,
			Errors:       notificationSyncMessage.Errors,
			DetectedAt:   detectedAt,
			RowID:        notificationSyncMessage.RowID,
		},
	)

	diffKeyboard := h.tgbtService.BuildApplicationDiffKeyboard(notificationSyncMessage.EventID)

	cacheKey := fmt.Sprintf("%s:%s", enums.CallbackPrefixApplicationDiff, notificationSyncMessage.EventID)
	ok, err := h.redisClient.Set(ctx, cacheKey, string(message.Value), applicationDiffCacheTTL)
	if err != nil {
		h.logger.Error("failed to set cache for application diff", "err", err)
		return err
	}
	if !ok {
		h.logger.Error("cache set operation returned false", "key", cacheKey)
		return fmt.Errorf("failed to set cache for key %s", cacheKey)
	}

	if err := h.tgbtService.SendMessage(ctx, diffMessage, diffKeyboard); err != nil {
		h.logger.Error("failed to send application diff message to telegram", "event_id", notificationSyncMessage.EventID, "err", err)
		return err
	}

	h.logger.Info("application diff notification sent to telegram", "event_id", notificationSyncMessage.EventID)

	return nil
}
