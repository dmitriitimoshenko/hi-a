package app

import (
	"context"
	"errors"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
	"golang.org/x/sync/errgroup"
)

const (
	maxConsumeRetries = 30

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
	consumeTopicsHandlers := s.consumeTopicsHandlers()
	if len(consumeTopicsHandlers) == 0 {
		return errors.New("no kafka consume topics configured")
	}

	g, gctx := errgroup.WithContext(ctx)

	for consumeTopic, consumeHandler := range consumeTopicsHandlers {
		topic := consumeTopic
		handler := consumeHandler

		g.Go(func() error {
			for attempt := 1; attempt <= maxConsumeRetries; attempt++ {
				s.logger.Info("starting consumer", slog.String("topic", topic), slog.Int("attempt", attempt))

				err := s.kafka.Consume(gctx, topic, handler)
				if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || gctx.Err() != nil {
					s.logger.Info("consumer stopped by context", slog.String("topic", topic), slog.Any("error", err))

					return nil
				}

				if attempt == maxConsumeRetries {
					s.logger.Error("consumer stopped after retries", slog.String("topic", topic), slog.Any("error", err), slog.Int("attempts", attempt))

					return err
				}

				s.logger.Error(
					"consumer failed, retrying",
					slog.String("topic", topic),
					slog.Any("error", err),
					slog.Int("attempt", attempt),
					slog.Int("max_attempts", maxConsumeRetries),
				)
			}

			return nil
		})
	}

	return g.Wait()
}

// consumeTopicsHandlers skips unset topics. Building the map straight from
// os.Getenv would subscribe to a topic named "" when a variable is missing,
// and two missing variables would collide on the same key, silently dropping
// a handler.
func (s *KafkaServer) consumeTopicsHandlers() map[string]func(context.Context, kafkaclient.Message) error {
	consumeTopicsHandlers := map[string]func(context.Context, kafkaclient.Message) error{}

	if topic := os.Getenv(KAFKA_TOPIC_NOTIFICATION); topic != "" {
		consumeTopicsHandlers[topic] = s.notificationHandler.Handle
	} else {
		s.logger.Warn("topic is not configured, consumer disabled", slog.String("env", KAFKA_TOPIC_NOTIFICATION))
	}

	if topic := os.Getenv(KAFKA_TOPIC_NOTIFICATION_SYNC); topic != "" {
		consumeTopicsHandlers[topic] = s.notificationSyncHandler.Handle
	} else {
		s.logger.Warn("topic is not configured, consumer disabled", slog.String("env", KAFKA_TOPIC_NOTIFICATION_SYNC))
	}

	return consumeTopicsHandlers
}

func (s *KafkaServer) Close(ctx context.Context) error {
	return s.kafka.Close(ctx)
}
