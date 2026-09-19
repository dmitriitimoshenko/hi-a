package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/bus"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/bus/handlers"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/redis"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/tgbt"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services"
	"golang.org/x/sync/errgroup"

	tgbot "github.com/go-telegram/bot"
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

	redisCfg := redis.LoadConfig()
	redisClient, err := redis.New(ctx, redisCfg)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	telegramBotHandler := tgbt.NewTelegramBotHandler(logger, redisClient, busClient)

	botConfig, err := tgbt.LoadConfig()
	if err != nil {
		return err
	}
	b, err := tgbot.New(
		botConfig.BotToken,
		tgbot.WithDefaultHandler(telegramBotHandler.Handle),
	)
	if err != nil {
		return err
	}
	botClient := tgbt.NewClient(*botConfig, b)

	tgbtService := services.NewTelegramBotService(botClient)

	notificationHandler := handlers.NewNotificationHandler(logger, redisClient, tgbtService)
	notificationSyncHandler := handlers.NewNotificationSyncHandler(logger, redisClient, tgbtService)

	busServer := app.NewBusServer(
		busClient,
		logger,
		notificationHandler,
		notificationSyncHandler,
	)

	g, gctx := errgroup.WithContext(ctx)

	defer busServer.Close(ctx)
	g.Go(func() error {
		return busServer.Run(gctx)
	})
	g.Go(func() error {
		b.Start(gctx)

		return nil
	})

	logger.Info("TB is running...")

	return g.Wait()
}
