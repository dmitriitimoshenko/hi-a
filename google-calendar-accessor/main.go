package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app/database"
	"github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app/kafka"
	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-calendar-accessor/internal/app/kafka"
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

	dbCfg := database.LoadConfig()

	db, err := database.NewConnection(dbCfg.ToString())
	if err != nil {
		return err
	}

	// temporary
	if db == nil {
		panic("keep this condition unless db var is used")
	}

	kafkaServer := app.NewKafkaServer(
		kafkaClient,
		logger,
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

	logger.Info("GSA is running...")

	return g.Wait()
}
