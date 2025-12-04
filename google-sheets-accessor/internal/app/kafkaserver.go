package app

import (
	"context"
	"errors"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	sheetsclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
)

const (
	maxConsumeRetries = 5

	KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING   = "KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"
	KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED = "KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"
)

type KafkaServer struct {
	kafka                             *kafkaclient.Client
	sheets                            *sheetsclient.Client
	logger                            *slog.Logger
	applicationUpdateProcessedHandler applicationUpdateProcessedHandler
	saveApplicationEmbeddingHandler   saveApplicationEmbeddingHandler
}

func NewKafkaServer(
	kafkaClient *kafkaclient.Client,
	sheetsClient *sheetsclient.Client,
	logger *slog.Logger,
	applicationUpdateProcessedHandler applicationUpdateProcessedHandler,
	saveApplicationEmbeddingHandler saveApplicationEmbeddingHandler,
) *KafkaServer {
	return &KafkaServer{
		kafka:                             kafkaClient,
		sheets:                            sheetsClient,
		logger:                            logger,
		applicationUpdateProcessedHandler: applicationUpdateProcessedHandler,
		saveApplicationEmbeddingHandler:   saveApplicationEmbeddingHandler,
	}
}

func (s *KafkaServer) Run(ctx context.Context) error {
	consumeTopicsHandlers := map[string]func(ctx context.Context, message kafkaclient.Message) error{
		os.Getenv("KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"):   s.saveApplicationEmbeddingHandler.Handle,
		os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"): s.applicationUpdateProcessedHandler.Handle,
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
