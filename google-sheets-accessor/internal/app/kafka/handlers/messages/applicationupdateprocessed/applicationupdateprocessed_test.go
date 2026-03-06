package applicationupdateprocessed_test

import (
	"encoding/json"
	"testing"

	aup "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka/handlers/messages/applicationupdateprocessed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNullableInt64UnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected *int64
		wantErr  bool
	}{
		{
			name:     "number value",
			input:    "3",
			expected: toInt64Ptr(3),
		},
		{
			name:     "numeric string",
			input:    `"4"`,
			expected: toInt64Ptr(4),
		},
		{
			name:     "null value",
			input:    "null",
			expected: nil,
		},
		{
			name:     "empty string treated as nil",
			input:    `""`,
			expected: nil,
		},
		{
			name:    "non numeric string",
			input:   `"stage-a"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var value aup.NullableInt64
			err := json.Unmarshal([]byte(tt.input), &value)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, value.Ptr())
		})
	}
}

func TestApplicationUpdatePayloadUnmarshalStageFromString(t *testing.T) {
	t.Parallel()

	payload := []byte(`{
		"email": {
			"id": 10,
			"label": "inbox",
			"subject": "subject",
			"body": "body",
			"content": "content",
			"sender_email": "sender@example.com",
			"sender_name": "Sender",
			"should_be_sent_to_heh": false
		},
		"mapped_application": {
			"id": 2,
			"row_id": 3,
			"company": "Acme Corp",
			"title": "Engineer",
			"employment_type": "full-time",
			"work_mode": "remote",
			"status": "applied",
			"stage": "7"
		},
		"stage": "9"
	}`)

	var message aup.ApplicationUpdatePayload
	err := json.Unmarshal(payload, &message)
	require.NoError(t, err)

	assert.Equal(t, toInt64Ptr(7), message.MappedApplication.Stage.Ptr())
	assert.Equal(t, toInt64Ptr(9), message.Stage.Ptr())
}

func toInt64Ptr(value int64) *int64 {
	return &value
}
