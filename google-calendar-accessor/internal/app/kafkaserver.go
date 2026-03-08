package app

import (
	"context"
	"errors"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app/kafka"
	"golang.org/x/sync/errgroup"
)

const (
	maxConsumeRetries = 30

	KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING   = "KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"
	KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED = "KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"
)

type KafkaServer struct {
	kafka  *kafkaclient.Client
	logger *slog.Logger
}

func NewKafkaServer(
	kafkaClient *kafkaclient.Client,
	logger *slog.Logger,
) *KafkaServer {
	return &KafkaServer{
		kafka:  kafkaClient,
		logger: logger,
	}
}

func (s *KafkaServer) Run(ctx context.Context) error {
	consumeTopicsHandlers := map[string]func(ctx context.Context, message kafkaclient.Message) error{
		// os.Getenv("KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"):   s.saveApplicationEmbeddingHandler.Handle,
		// os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"): s.applicationUpdateProcessedHandler.Handle,
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

func (s *KafkaServer) Close(ctx context.Context) error {
	return s.kafka.Close(ctx)
}
