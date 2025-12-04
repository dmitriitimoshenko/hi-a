package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/database"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka/handlers"
	sheetsclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/repositories"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services"
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

	sheetsCfg, err := sheetsclient.LoadConfig()
	if err != nil {
		return err
	}

	sheetsClient, err := sheetsclient.New(ctx, sheetsCfg)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dbCfg := database.LoadConfig()

	db, err := database.NewConnection(dbCfg.ToString())
	if err != nil {
		return err
	}

	salaryRepository := repositories.NewSalaryRepository(db)
	applicationRepository := repositories.NewApplicationRepository(db, logger)

	sheetsService := services.NewSheetsService(sheetsClient)
	salaryService := services.NewSalaryService(salaryRepository)
	applicationService := services.NewApplicationService(db, kafkaClient, sheetsService, applicationRepository, salaryService)

	saveApplicationEmbeddingHandler := handlers.NewSaveApplicationEmbeddingHandler(applicationService)
	applicationUpdateProcessedHandler := handlers.NewApplicationUpdateProcessedHandler(applicationService)

	kafkaServer := app.NewKafkaServer(
		kafkaClient,
		sheetsClient,
		logger,
		applicationUpdateProcessedHandler,
		saveApplicationEmbeddingHandler,
	)

	httpServer := app.NewHTTPServer(applicationService, sheetsService)

	g, gctx := errgroup.WithContext(ctx)

	defer kafkaServer.Close(ctx)
	g.Go(func() error {
		return kafkaServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	return g.Wait()
}
