package tools_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
	"gorm.io/datatypes"
)

func TestFromJSONMap(t *testing.T) {
	t.Parallel()
	t.Run("nil_input", func(t *testing.T) {
		assert.Equal(t, tools.FromJSONMap(nil), (map[string]string)(nil))
	})

	t.Run("typed_nil_input", func(t *testing.T) {
		var src datatypes.JSONMap

		assert.Equal(t, tools.FromJSONMap(&src), map[string]string{})
	})

	t.Run("mixed_values", func(t *testing.T) {
		src := datatypes.JSONMap{
			"str":  "value",
			"num":  10,
			"none": nil,
		}

		result := tools.FromJSONMap(&src)

		assert.Equal(t, result["str"], "value")
		assert.Equal(t, result["num"], "10")
		_, exists := result["none"]
		assert.Equal(t, exists, false)
	})
}
