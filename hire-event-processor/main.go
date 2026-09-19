package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata"

	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus/handlers"
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

	busConfig, err := bus.LoadConfig()
	if err != nil {
		return err
	}

	busClient, err := bus.New(busConfig)
	if err != nil {
		return err
	}

	interestingMailHandler := handlers.NewInterestingMailHandler(
		logger,
		busClient,
		appConfig.TopicHireEvent,
	)
	applicationUpdateTransformer := handlers.NewApplicationUpdateTransformer(logger)
	applicationUpdateHandler := handlers.NewApplicationUpdateHandler(
		logger,
		busClient,
		applicationUpdateTransformer,
		appConfig.TopicApplicationUpdateProcessed,
	)
	applicationSyncHandler := handlers.NewApplicationSyncHandler(
		logger,
		busClient,
		appConfig.TopicApplicationsSyncProcessed,
	)

	busServer := app.NewBusServer(
		logger,
		appConfig,
		busClient,
		interestingMailHandler,
		applicationUpdateHandler,
		applicationSyncHandler,
	)
	httpServer := app.NewHTTPServer(logger, appConfig)

	g, gctx := errgroup.WithContext(ctx)

	defer busServer.Close(ctx)
	g.Go(func() error {
		return busServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	logger.Info("HEP is running...")

	return g.Wait()
}
