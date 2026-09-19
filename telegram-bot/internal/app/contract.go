package app

import (
	"context"

	busclient "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/bus"
)

type notificationHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}

type notificationSyncHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}
