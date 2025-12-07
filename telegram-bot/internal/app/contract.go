package app

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka"
)

type notificationHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type notificationSyncHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}
