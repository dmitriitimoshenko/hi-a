package app

import (
	"context"
	"errors"
	"log/slog"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
	"golang.org/x/sync/errgroup"
)

const maxConsumeRetries = 30

type BusServer struct {
	logger                   *slog.Logger
	config                   *Config
	bus                      *busclient.Client
	interestingMailHandler   interestingMailHandler
	applicationUpdateHandler applicationUpdateHandler
	applicationSyncHandler   applicationSyncHandler
}

func NewBusServer(
	logger *slog.Logger,
	config *Config,
	busClient *busclient.Client,
	interestingMailHandler interestingMailHandler,
	applicationUpdateHandler applicationUpdateHandler,
	applicationSyncHandler applicationSyncHandler,
) *BusServer {
	return &BusServer{
		logger:                   logger,
		config:                   config,
		bus:                      busClient,
		interestingMailHandler:   interestingMailHandler,
		applicationUpdateHandler: applicationUpdateHandler,
		applicationSyncHandler:   applicationSyncHandler,
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

func (s *BusServer) Close(ctx context.Context) error {
	return s.bus.Close(ctx)
}

func (s *BusServer) consumeTopicsHandlers() map[string]func(context.Context, busclient.Message) error {
	consumeTopicsHandlers := map[string]func(context.Context, busclient.Message) error{}

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
