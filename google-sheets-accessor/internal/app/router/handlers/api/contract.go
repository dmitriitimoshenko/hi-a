package api

import (
	"context"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error)
	Fetch(ctx context.Context) (int64, int64, error)
}
