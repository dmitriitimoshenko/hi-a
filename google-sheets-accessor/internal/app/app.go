package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	sheetsclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
	"golang.org/x/sync/errgroup"
)

const (
	maxConsumeRetries = 5

	KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING   = "KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"
	KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED = "KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"
)

type App struct {
	kafka  *kafkaclient.Client
	sheets *sheetsclient.Client
	logger *slog.Logger
}

func New(kafkaClient *kafkaclient.Client, sheetsClient *sheetsclient.Client) *App {
	return &App{
		kafka:  kafkaClient,
		sheets: sheetsClient,
		logger: slog.Default(),
	}
}

func (a *App) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		if err := RunHTTPServer(ctx); err != nil {
			return fmt.Errorf("router failed: %w", err)
		}
		return nil
	})

	consumeTopics := []string{
		os.Getenv(KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING),
		os.Getenv(KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED),
	}

	for _, consumeTopic := range consumeTopics {
		if consumeTopic == "" {
			continue
		}

		consumeTopic := consumeTopic
		group.Go(func() error {
			for attempt := 1; attempt <= maxConsumeRetries; attempt++ {
				err := a.kafka.Consume(ctx, consumeTopic, a.handleMessage)
				if err == nil {
					return nil
				}

				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					a.logger.Info("consumer stopped by context", slog.String("topic", consumeTopic), slog.Any("error", err))

					return nil
				}

				if attempt == maxConsumeRetries {
					a.logger.Error("consumer stopped after retries", slog.String("topic", consumeTopic), slog.Any("error", err), slog.Int("attempts", attempt))

					return err
				}

				a.logger.Error(
					"consumer failed, retrying",
					slog.String("topic", consumeTopic),
					slog.Any("error", err),
					slog.Int("attempt", attempt),
					slog.Int("max_attempts", maxConsumeRetries),
				)
			}
			return nil
		})
	}

	return group.Wait()
}

func (a *App) handleMessage(ctx context.Context, message kafkaclient.Message) error {
	err := a.kafka.Publish(ctx, "topic", message.Key, message.Value)
	if err != nil {
		return fmt.Errorf("failed to publish forward: %w", err)
	}

	return nil
}

func (a *App) Close(ctx context.Context) error {
	return a.kafka.Close(ctx)
}
