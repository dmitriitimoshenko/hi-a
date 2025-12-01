package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	sheetsclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
)

const maxConsumeRetries = 5

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
	consumeTopicsHandlers := map[string]func(ctx context.Context, message kafkaclient.Message) error{
		os.Getenv("KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"):   a.handleMessage,
		os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED"): a.handleMessage,
	}

	for consumeTopic, consumeHandler := range consumeTopicsHandlers {
		go func(ctx context.Context, consumeTopic string, consumeHandler func(ctx context.Context, message kafkaclient.Message) error) {
			for attempt := 1; attempt <= maxConsumeRetries; attempt++ {
				err := a.kafka.Consume(ctx, consumeTopic, consumeHandler)
				if err == nil {
					return
				}

				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					a.logger.Info("consumer stopped by context", slog.String("topic", consumeTopic), slog.Any("error", err))

					return
				}

				if attempt == maxConsumeRetries {
					a.logger.Error("consumer stopped after retries", slog.String("topic", consumeTopic), slog.Any("error", err), slog.Int("attempts", attempt))

					return
				}

				a.logger.Error(
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
