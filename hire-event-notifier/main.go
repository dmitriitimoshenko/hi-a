package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/bus"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/bus/handlers"
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

	busCfg, err := bus.LoadConfig()
	if err != nil {
		return err
	}

	busClient, err := bus.New(busCfg)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	applicationsSyncProcessedHandler := handlers.NewApplicationUpdateProcessedHandler(
		logger,
		busClient,
	)
	hireEventHandler := handlers.NewHireEventHandler(
		logger,
		busClient,
	)

	busServer := app.NewBusServer(
		logger,
		busClient,
		hireEventHandler,
		applicationsSyncProcessedHandler,
	)

	httpServer := app.NewHTTPServer(logger)

	g, gctx := errgroup.WithContext(ctx)

	defer busServer.Close(ctx)
	g.Go(func() error {
		return busServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	logger.Info("HEN is running...")

	return g.Wait()
}
