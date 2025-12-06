package tools_test

import (
	"testing"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/magiconair/properties/assert"
)

func TestRemoveLetters(t *testing.T) {
	assert.Equal(t, tools.RemoveLetters("abcXYZ123!@#"), "123!@#")
}
