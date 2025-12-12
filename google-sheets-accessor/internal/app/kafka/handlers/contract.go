package handlers

import (
	"context"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
)

type applicationService interface {
	UpdateAndSync(ctx context.Context, application dto.UpdateApplicationDTO) error
	AddEmbeddingByID(ctx context.Context, id int64, embedding []float32) error
	FindByID(ctx context.Context, id int64) (*models.Application, error)
}
