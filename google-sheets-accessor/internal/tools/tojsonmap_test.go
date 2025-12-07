package tools_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
	"gorm.io/datatypes"
)

func TestToJSONMap(t *testing.T) {
	data := map[string]string{
		"foo": "bar",
		"baz": "qux",
	}

	jm := tools.ToJSONMap(data)

	expected := datatypes.JSONMap{
		"foo": "bar",
		"baz": "qux",
	}

	assert.Equal(t, *jm, expected)
}
