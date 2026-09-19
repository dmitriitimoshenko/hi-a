package app

import (
	"context"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/bus"
)

type hireEventHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}

type applicationSyncProcessedHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}
