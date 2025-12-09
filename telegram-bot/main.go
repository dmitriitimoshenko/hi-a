package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka/handlers"
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

	kafkaCfg, err := kafka.LoadConfig()
	if err != nil {
		return err
	}

	kafkaClient, err := kafkaclient.New(kafkaCfg)
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

	telegramBotHandler := tgbt.NewTelegramBotHandler(logger, redisClient, kafkaClient)

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

	kafkaServer := app.NewKafkaServer(
		kafkaClient,
		logger,
		notificationHandler,
		notificationSyncHandler,
	)

	g, gctx := errgroup.WithContext(ctx)

	defer kafkaServer.Close(ctx)
	g.Go(func() error {
		return kafkaServer.Run(gctx)
	})

	logger.Info("TB is running...")

	return g.Wait()
}
