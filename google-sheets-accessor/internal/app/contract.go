package app

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error)
}

type applicationUpdateProcessedHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type saveApplicationEmbeddingHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}
