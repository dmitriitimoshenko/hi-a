package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	sheetsclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
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

	application := app.New(kafkaClient, sheetsClient)
	defer application.Close(ctx)

	return application.Run(ctx)
}
