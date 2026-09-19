package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/bus"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/bus/handlers"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/database"
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

	busCfg, err := bus.LoadConfig()
	if err != nil {
		return err
	}

	busClient, err := bus.New(busCfg)
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
	applicationRepository := repositories.NewApplicationRepository(logger, db)

	sheetsService := services.NewSheetsService(logger, sheetsClient)
	salaryService := services.NewSalaryService(salaryRepository)
	applicationService := services.NewApplicationService(
		db,
		logger,
		busClient,
		sheetsService,
		applicationRepository,
		salaryService,
	)

	saveApplicationEmbeddingHandler := handlers.NewSaveApplicationEmbeddingHandler(logger, applicationService)
	applicationUpdateProcessedHandler := handlers.NewApplicationUpdateProcessedHandler(logger, applicationService)

	busServer := app.NewBusServer(
		busClient,
		sheetsClient,
		logger,
		applicationUpdateProcessedHandler,
		saveApplicationEmbeddingHandler,
	)

	httpServer := app.NewHTTPServer(applicationService, sheetsService, logger)

	g, gctx := errgroup.WithContext(ctx)

	defer busServer.Close(ctx)
	g.Go(func() error {
		return busServer.Run(gctx)
	})

	g.Go(func() error {
		return httpServer.Run(gctx)
	})

	logger.Info("GSA is running...")

	return g.Wait()
}
