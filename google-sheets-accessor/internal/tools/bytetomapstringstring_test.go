package tools_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
)

func TestByteToMapStringString(t *testing.T) {
	t.Parallel()
	t.Run("valid_json", func(t *testing.T) {
		meta, err := tools.ByteToMapStringString([]byte(`{"a":"b"}`))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if meta == nil {
			t.Fatalf("expected meta not to be nil")
		}

		assert.Equal(t, (*meta)["a"], "b")
	})

	t.Run("empty_input", func(t *testing.T) {
		meta, err := tools.ByteToMapStringString([]byte{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		assert.Equal(t, len(*meta), 0)
	})

	t.Run("invalid_json", func(t *testing.T) {
		meta, err := tools.ByteToMapStringString([]byte(`{]`))

		assert.Equal(t, err == nil, false)
		assert.Equal(t, meta == nil, true)
	})
}
