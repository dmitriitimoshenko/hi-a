package tools_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
	"github.com/pgvector/pgvector-go"
)

func TestEmbeddingToSlice(t *testing.T) {
	vec := pgvector.NewVector([]float32{1.1, 2.2})

	assert.Equal(t, tools.EmbeddingToSlice(&vec), []float32{1.1, 2.2})
	assert.Equal(t, tools.EmbeddingToSlice(nil), []float32(nil))
}
