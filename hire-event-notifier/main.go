package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka/handlers"
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

	kafkaCfg, err := kafka.LoadConfig()
	if err != nil {
		return err
	}

	kafkaClient, err := kafkaclient.New(kafkaCfg)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	applicationsSyncProcessedHandler := handlers.NewApplicationUpdateProcessedHandler(
		logger,
		kafkaClient,
	)
	hireEventHandler := handlers.NewHireEventHandler(
		logger,
		kafkaClient,
	)

	kafkaServer := app.NewKafkaServer(
		logger,
		kafkaClient,
		hireEventHandler,
		applicationsSyncProcessedHandler,
	)

	httpServer := app.NewHTTPServer(logger)

	g, gctx := errgroup.WithContext(ctx)

	defer kafkaServer.Close(ctx)
	g.Go(func() error {
		return kafkaServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	logger.Info("HEN is running...")

	return g.Wait()
}
