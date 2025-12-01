package services

import (
	"context"
	"fmt"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/pgvector/pgvector-go"
)

type repository interface {
	FindByID(ctx context.Context, id int64) (*models.Application, error)
	Save(ctx context.Context, application *models.Application) error
}

type ApplicationService struct {
	repository repository
}

func NewApplicationService(
	repository repository,
) *ApplicationService {
	return &ApplicationService{
		repository: repository,
	}
}

func (s *ApplicationService) AddEmbeddingByID(ctx context.Context, id int64, embedding []float32) error {
	application, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if application == nil {
		return fmt.Errorf("failed to AddEmbeddingByID: no application found by id [%d]", id)
	}

	embeddingVector := pgvector.NewVector(embedding)
	application.Embedding = &embeddingVector

	if err = s.repository.Save(ctx, application); err != nil {
		return err
	}

	return nil
}
