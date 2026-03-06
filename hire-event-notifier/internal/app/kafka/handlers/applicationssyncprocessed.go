package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka/handlers/messages"
)

type ApplicationUpdateProcessedHandler struct {
	logger *slog.Logger
	kafka  *kafkaclient.Client
}

func NewApplicationUpdateProcessedHandler(logger *slog.Logger, kafkaClient *kafkaclient.Client) *ApplicationUpdateProcessedHandler {
	return &ApplicationUpdateProcessedHandler{
		logger: logger,
		kafka:  kafkaClient,
	}
}

func (h *ApplicationUpdateProcessedHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	h.logger.Info("handling applications-sync-processed message")

	var payload messages.ApplicationSyncProcessedMessage
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		h.logger.Error("failed to unmarshal applications-sync-processed message", slog.Any("error", err))

		return err
	}

	rowID := payload.RowID.Ptr()
	applicationID := payload.ApplicationID.Ptr()
	if rowID == nil && applicationID == nil {
		err := errors.New("missing application_id or row_id in applications-sync-processed payload")
		h.logger.Error("cannot determine kafka key for applications-sync-processed payload", slog.Any("error", err))

		return err
	}

	if rowID == nil {
		err := errors.New("row_id is required for notification-sync payload")
		h.logger.Error("applications-sync-processed payload is missing row_id", slog.Any("error", err))

		return err
	}

	keyValue := rowID
	if applicationID != nil {
		keyValue = applicationID
	}

	eventID, err := h.newEventID()
	if err != nil {
		h.logger.Error("failed to generate event id", slog.Any("error", err))

		return err
	}

	notificationPayload := messages.NotificationSyncMessage{
		EventID:     eventID,
		Type:        "application_diff",
		RowID:       *rowID,
		Company:     payload.Company,
		RoleTitle:   payload.RoleTitle,
		Differences: payload.Differences,
		Errors:      payload.Errors,
		DetectedAt:  payload.DetectedAt,
	}

	payloadBytes, err := json.Marshal(notificationPayload)
	if err != nil {
		h.logger.Error("failed to marshal notification-sync payload", slog.Any("error", err))

		return err
	}

	keyBytes := []byte(strconv.FormatInt(*keyValue, 10))

	notificationSyncTopic := os.Getenv("KAFKA_TOPIC_NOTIFICATION_SYNC")
	if notificationSyncTopic == "" {
		return errors.New("KAFKA_TOPIC_NOTIFICATION_SYNC is required")
	}

	if err := h.kafka.Publish(ctx, notificationSyncTopic, keyBytes, payloadBytes); err != nil {
		h.logger.Error(
			"failed to publish notification-sync message",
			slog.String("topic", notificationSyncTopic),
			slog.Any("error", err),
		)

		return err
	}

	h.logger.Info(
		"notification-sync message published",
		slog.String("topic", notificationSyncTopic),
		slog.String("event_id", eventID),
	)

	return nil
}

func (h *ApplicationUpdateProcessedHandler) newEventID() (string, error) {
	uuidBytes := make([]byte, 16)
	if _, err := rand.Read(uuidBytes); err != nil {
		return "", fmt.Errorf("failed to read random bytes for uuid: %w", err)
	}

	uuidBytes[6] = (uuidBytes[6] & 0x0f) | 0x40
	uuidBytes[8] = (uuidBytes[8] & 0x3f) | 0x80

	hexEncoded := hex.EncodeToString(uuidBytes)

	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hexEncoded[0:8],
		hexEncoded[8:12],
		hexEncoded[12:16],
		hexEncoded[16:20],
		hexEncoded[20:32],
	), nil
}
