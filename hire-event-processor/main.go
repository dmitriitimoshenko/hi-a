package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata"

	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka/handlers"
	"golang.org/x/sync/errgroup"
)

func main() {
	err := run()
	if err != nil {
		slog.Error("application failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	appConfig := app.LoadConfig()

	kafkaConfig, err := kafka.LoadConfig()
	if err != nil {
		return err
	}

	kafkaClient, err := kafka.New(kafkaConfig)
	if err != nil {
		return err
	}

	interestingMailHandler := handlers.NewInterestingMailHandler(
		logger,
		kafkaClient,
		appConfig.TopicHireEvent,
	)
	applicationUpdateTransformer := handlers.NewApplicationUpdateTransformer(logger)
	applicationUpdateHandler := handlers.NewApplicationUpdateHandler(
		logger,
		kafkaClient,
		applicationUpdateTransformer,
		appConfig.TopicApplicationUpdateProcessed,
	)
	applicationSyncHandler := handlers.NewApplicationSyncHandler(
		logger,
		kafkaClient,
		appConfig.TopicApplicationsSyncProcessed,
	)

	kafkaServer := app.NewKafkaServer(
		logger,
		appConfig,
		kafkaClient,
		interestingMailHandler,
		applicationUpdateHandler,
		applicationSyncHandler,
	)
	httpServer := app.NewHTTPServer(logger, appConfig)

	g, gctx := errgroup.WithContext(ctx)

	defer kafkaServer.Close(ctx)
	g.Go(func() error {
		return kafkaServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	logger.Info("HEP is running...")

	return g.Wait()
}
