package app

import (
	"context"
	"errors"
	"log/slog"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
	"golang.org/x/sync/errgroup"
)

const maxConsumeRetries = 30

type KafkaServer struct {
	logger                   *slog.Logger
	config                   *Config
	kafka                    *kafkaclient.Client
	interestingMailHandler   interestingMailHandler
	applicationUpdateHandler applicationUpdateHandler
	applicationSyncHandler   applicationSyncHandler
}

func NewKafkaServer(
	logger *slog.Logger,
	config *Config,
	kafkaClient *kafkaclient.Client,
	interestingMailHandler interestingMailHandler,
	applicationUpdateHandler applicationUpdateHandler,
	applicationSyncHandler applicationSyncHandler,
) *KafkaServer {
	return &KafkaServer{
		logger:                   logger,
		config:                   config,
		kafka:                    kafkaClient,
		interestingMailHandler:   interestingMailHandler,
		applicationUpdateHandler: applicationUpdateHandler,
		applicationSyncHandler:   applicationSyncHandler,
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

func (s *KafkaServer) Close(ctx context.Context) error {
	return s.kafka.Close(ctx)
}

func (s *KafkaServer) consumeTopicsHandlers() map[string]func(context.Context, kafkaclient.Message) error {
	consumeTopicsHandlers := map[string]func(context.Context, kafkaclient.Message) error{}

	if s.config.TopicInterestingMail != "" {
		consumeTopicsHandlers[s.config.TopicInterestingMail] = s.interestingMailHandler.Handle
	}

	if s.config.TopicApplicationUpdateUnprocessed != "" {
		consumeTopicsHandlers[s.config.TopicApplicationUpdateUnprocessed] = s.applicationUpdateHandler.Handle
	}

	if s.config.TopicApplicationsSyncUnprocessed != "" {
		consumeTopicsHandlers[s.config.TopicApplicationsSyncUnprocessed] = s.applicationSyncHandler.Handle
	}

	return consumeTopicsHandlers
}
