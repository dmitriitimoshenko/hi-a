package app

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-notifier/internal/app/kafka"
)

type hireEventHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type applicationSyncProcessedHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}
