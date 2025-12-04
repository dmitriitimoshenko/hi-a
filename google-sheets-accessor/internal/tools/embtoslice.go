package tools

import "github.com/pgvector/pgvector-go"

func EmbeddingToSlice(v *pgvector.Vector) []float32 {
	if v != nil {
		return v.Slice()
	}

	return nil
}
