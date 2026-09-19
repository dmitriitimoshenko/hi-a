package app

import (
	"context"
	"errors"
	"log/slog"
	"os"

	busclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/bus"
	"golang.org/x/sync/errgroup"
)

const (
	maxConsumeRetries = 30

	STREAM_NOTIFICATION      = "STREAM_NOTIFICATION"
	STREAM_NOTIFICATION_SYNC = "STREAM_NOTIFICATION_SYNC"
)

type BusServer struct {
	bus                     *busclient.Client
	logger                  *slog.Logger
	notificationHandler     notificationHandler
	notificationSyncHandler notificationSyncHandler
}

func NewBusServer(
	busClient *busclient.Client,
	logger *slog.Logger,
	notificationHandler notificationHandler,
	notificationSyncHandler notificationSyncHandler,
) *BusServer {
	return &BusServer{
		bus:                     busClient,
		logger:                  logger,
		notificationHandler:     notificationHandler,
		notificationSyncHandler: notificationSyncHandler,
	}
}

func (s *BusServer) Run(ctx context.Context) error {
	consumeTopicsHandlers := s.consumeTopicsHandlers()
	if len(consumeTopicsHandlers) == 0 {
		return errors.New("no bus consume streams configured")
	}

	g, gctx := errgroup.WithContext(ctx)

	for consumeTopic, consumeHandler := range consumeTopicsHandlers {
		topic := consumeTopic
		handler := consumeHandler

		g.Go(func() error {
			for attempt := 1; attempt <= maxConsumeRetries; attempt++ {
				s.logger.Info("starting consumer", slog.String("topic", topic), slog.Int("attempt", attempt))

				err := s.bus.Consume(gctx, topic, handler)
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

// consumeTopicsHandlers skips unset streams. Building the map straight from
// os.Getenv would subscribe to a stream named "" when a variable is missing,
// and two missing variables would collide on the same key, silently dropping
// a handler.
func (s *BusServer) consumeTopicsHandlers() map[string]func(context.Context, busclient.Message) error {
	consumeTopicsHandlers := map[string]func(context.Context, busclient.Message) error{}

	if topic := os.Getenv(STREAM_NOTIFICATION); topic != "" {
		consumeTopicsHandlers[topic] = s.notificationHandler.Handle
	} else {
		s.logger.Warn("stream is not configured, consumer disabled", slog.String("env", STREAM_NOTIFICATION))
	}

	if topic := os.Getenv(STREAM_NOTIFICATION_SYNC); topic != "" {
		consumeTopicsHandlers[topic] = s.notificationSyncHandler.Handle
	} else {
		s.logger.Warn("stream is not configured, consumer disabled", slog.String("env", STREAM_NOTIFICATION_SYNC))
	}

	return consumeTopicsHandlers
}

func (s *BusServer) Close(ctx context.Context) error {
	return s.bus.Close(ctx)
}
