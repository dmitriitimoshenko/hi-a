package app

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka"
)

type interestingMailHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type applicationUpdateHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type applicationSyncHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}
