package handlers_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/kafka/handlers"
)

func TestApplicationUpdateTransformerTransform(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		payload  string
		validate func(t *testing.T, payload map[string]any)
	}{
		{
			name: "applies denied confirmation",
			payload: `{
				"action":"cnfm",
				"email":{"label":"denied"},
				"mapped_application":{"status":"applied"}
			}`,
			validate: func(t *testing.T, payload map[string]any) {
				t.Helper()

				if payload["status"] != "denied" {
					t.Fatalf("expected payload status to be denied, got %v", payload["status"])
				}

				mappedApplication := getObject(t, payload, "mapped_application")
				if mappedApplication["status"] != "denied" {
					t.Fatalf("expected mapped_application status to be denied, got %v", mappedApplication["status"])
				}

				respondedAt := getString(t, payload, "responded_at")
				mappedRespondedAt := getString(t, mappedApplication, "responded_at")
				if respondedAt == "" || mappedRespondedAt == "" {
					t.Fatalf("expected responded_at to be populated")
				}

				if respondedAt != mappedRespondedAt {
					t.Fatalf("expected responded_at values to match, got payload=%s mapped=%s", respondedAt, mappedRespondedAt)
				}
			},
		},
		{
			name: "increments stage and enriches meeting creation",
			payload: `{
				"action":"cnfm",
				"email":{
					"id":42,
					"label":"meeting_crt",
					"ics_file_data_list":[
						{"content":"BEGIN:VCALENDAR\r\nDTSTART:20260102T150405Z\r\nEND:VCALENDAR"}
					]
				},
				"mapped_application":{
					"id":10,
					"row_id":"20",
					"stage":"2",
					"status":"applied"
				}
			}`,
			validate: func(t *testing.T, payload map[string]any) {
				t.Helper()

				if got := getInt64(t, payload, "stage"); got != 3 {
					t.Fatalf("expected payload stage to be 3, got %d", got)
				}

				mappedApplication := getObject(t, payload, "mapped_application")
				if got := getInt64(t, mappedApplication, "stage"); got != 3 {
					t.Fatalf("expected mapped_application stage to be 3, got %d", got)
				}

				if payload["status"] != "meeting" {
					t.Fatalf("expected payload status to be meeting, got %v", payload["status"])
				}

				if mappedApplication["status"] != "meeting" {
					t.Fatalf("expected mapped_application status to be meeting, got %v", mappedApplication["status"])
				}

				respondedAt := getString(t, payload, "responded_at")
				if respondedAt == "" {
					t.Fatalf("expected responded_at to be populated")
				}

				nextFollowUpAt := getString(t, payload, "next_follow_up_at")
				if nextFollowUpAt != "2026-01-02T15:04:05+00:00" {
					t.Fatalf("expected next_follow_up_at to be derived from ICS, got %s", nextFollowUpAt)
				}
			},
		},
		{
			name: "keeps forbidden meeting status unchanged",
			payload: `{
				"action":"cnfm",
				"email":{"label":"meeting_inv"},
				"mapped_application":{
					"status":"offer",
					"responded_at":"2026-01-01T00:00:00+00:00"
				}
			}`,
			validate: func(t *testing.T, payload map[string]any) {
				t.Helper()

				mappedApplication := getObject(t, payload, "mapped_application")
				if mappedApplication["status"] != "offer" {
					t.Fatalf("expected mapped_application status to stay offer, got %v", mappedApplication["status"])
				}

				if _, ok := payload["status"]; ok {
					t.Fatalf("expected payload status to stay unset, got %v", payload["status"])
				}

				if getString(t, payload, "responded_at") != "2026-01-01T00:00:00+00:00" {
					t.Fatalf("expected responded_at to stay unchanged")
				}

				if _, ok := payload["next_follow_up_at"]; ok {
					t.Fatalf("expected next_follow_up_at to stay unset, got %v", payload["next_follow_up_at"])
				}
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
			transformer := handlers.NewApplicationUpdateTransformer(logger)

			transformedPayload, err := transformer.Transform([]byte(testCase.payload))
			if err != nil {
				t.Fatalf("transform returned error: %v", err)
			}

			payload := decodeObject(t, transformedPayload)

			testCase.validate(t, payload)
		})
	}
}

func decodeObject(t *testing.T, payloadBytes []byte) map[string]any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.UseNumber()

	var payload map[string]any

	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}

	return payload
}

func getObject(t *testing.T, payload map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := payload[key]
	if !ok {
		t.Fatalf("expected key %s to exist", key)
	}

	objectValue, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected key %s to be an object, got %T", key, value)
	}

	return objectValue
}

func getString(t *testing.T, payload map[string]any, key string) string {
	t.Helper()

	value, ok := payload[key]
	if !ok {
		t.Fatalf("expected key %s to exist", key)
	}

	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("expected key %s to be a string, got %T", key, value)
	}

	return stringValue
}

func getInt64(t *testing.T, payload map[string]any, key string) int64 {
	t.Helper()

	value, ok := payload[key]
	if !ok {
		t.Fatalf("expected key %s to exist", key)
	}

	numberValue, ok := value.(json.Number)
	if !ok {
		t.Fatalf("expected key %s to be json.Number, got %T", key, value)
	}

	parsedValue, err := numberValue.Int64()
	if err != nil {
		t.Fatalf("failed to parse %s as int64: %v", key, err)
	}

	return parsedValue
}
