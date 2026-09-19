package app

import (
	"context"

	busclient "github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/bus"
)

type interestingMailHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}

type applicationUpdateHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}

type applicationSyncHandler interface {
	Handle(ctx context.Context, message busclient.Message) error
}
