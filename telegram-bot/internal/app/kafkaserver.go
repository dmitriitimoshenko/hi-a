package app

import (
	"context"
	"errors"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
)

const (
	maxConsumeRetries = 5

	KAFKA_TOPIC_NOTIFICATION      = "KAFKA_TOPIC_NOTIFICATION"
	KAFKA_TOPIC_NOTIFICATION_SYNC = "KAFKA_TOPIC_NOTIFICATION_SYNC"
)

type KafkaServer struct {
	kafka                   *kafkaclient.Client
	logger                  *slog.Logger
	notificationHandler     notificationHandler
	notificationSyncHandler notificationSyncHandler
}

func NewKafkaServer(
	kafkaClient *kafkaclient.Client,
	logger *slog.Logger,
	notificationHandler notificationHandler,
	notificationSyncHandler notificationSyncHandler,
) *KafkaServer {
	return &KafkaServer{
		kafka:                   kafkaClient,
		logger:                  logger,
		notificationHandler:     notificationHandler,
		notificationSyncHandler: notificationSyncHandler,
	}
}

func (s *KafkaServer) Run(ctx context.Context) error {
	consumeTopicsHandlers := map[string]func(ctx context.Context, message kafkaclient.Message) error{
		os.Getenv("KAFKA_TOPIC_NOTIFICATION"):      s.notificationHandler.Handle,
		os.Getenv("KAFKA_TOPIC_NOTIFICATION_SYNC"): s.notificationSyncHandler.Handle,
	}

	for consumeTopic, consumeHandler := range consumeTopicsHandlers {
		go func(ctx context.Context, consumeTopic string, consumeHandler func(ctx context.Context, message kafkaclient.Message) error) {
			for attempt := 1; attempt <= maxConsumeRetries; attempt++ {
				err := s.kafka.Consume(ctx, consumeTopic, consumeHandler)
				if err == nil {
					s.logger.Info(
						"consumer started",
						slog.String("topic", consumeTopic),
						slog.Int("attempt", attempt),
						slog.Int("max_attempts", maxConsumeRetries),
					)
					return
				}

				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					s.logger.Info("consumer stopped by context", slog.String("topic", consumeTopic), slog.Any("error", err))

					return
				}

				if attempt == maxConsumeRetries {
					s.logger.Error("consumer stopped after retries", slog.String("topic", consumeTopic), slog.Any("error", err), slog.Int("attempts", attempt))

					return
				}

				s.logger.Error(
					"consumer failed, retrying",
					slog.String("topic", consumeTopic),
					slog.Any("error", err),
					slog.Int("attempt", attempt),
					slog.Int("max_attempts", maxConsumeRetries),
				)
			}
		}(ctx, consumeTopic, consumeHandler)
	}

	return nil
}

func (s *KafkaServer) Close(ctx context.Context) error {
	return s.kafka.Close(ctx)
}
