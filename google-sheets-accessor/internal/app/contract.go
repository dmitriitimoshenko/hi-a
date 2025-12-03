package app

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error)
	Fetch(ctx context.Context) (int64, int64, error)
	GetMaxRowID(ctx context.Context) (*int64, error)
	List(
		ctx context.Context,
		applicationStatusInclude []enums.ApplicationStatus,
		applicationStatusExclude []enums.ApplicationStatus,
		isReplyEmailReceived bool,
	) ([]*models.Application, error)
}

type applicationUpdateProcessedHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type saveApplicationEmbeddingHandler interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}

type saveApplicationEmbedding interface {
	Handle(ctx context.Context, message kafkaclient.Message) error
}
