package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const meetingCreationLabel = "meeting_crt"

var (
	meetingConfirmationLabels = map[string]struct{}{
		"meeting_crt": {},
		"meeting_inv": {},
	}
	dtStartPattern = regexp.MustCompile(`(?i)DTSTART(?:;TZID=([^:]+))?:([0-9]{8}T[0-9]{6})(Z)?`)
)

type ApplicationUpdateTransformer struct {
	logger *slog.Logger
}

func NewApplicationUpdateTransformer(logger *slog.Logger) *ApplicationUpdateTransformer {
	return &ApplicationUpdateTransformer{
		logger: logger,
	}
}

func (t *ApplicationUpdateTransformer) Transform(payloadBytes []byte) ([]byte, error) {
	payload, err := decodeJSONObject(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode application-update payload: %w", err)
	}

	t.applyDeniedConfirmation(payload)
	t.updateStageIfNeeded(payload)
	t.enrichMeetingConfirmation(payload)

	transformedPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode application-update payload: %w", err)
	}

	return transformedPayload, nil
}

func (t *ApplicationUpdateTransformer) applyDeniedConfirmation(payload map[string]any) {
	if !t.isDeniedConfirmation(payload) {
		return
	}

	mappedApplication, mappedApplicationOK := getObject(payload, "mapped_application")
	if !mappedApplicationOK {
		t.logger.Warn("skipping mapped_application update for denied confirmation: missing or invalid")
	}

	payload["status"] = "denied"
	if mappedApplicationOK {
		mappedApplication["status"] = "denied"
	}

	respondedAt, hasRespondedAt := getNonEmptyValue(payload, "responded_at")
	if !hasRespondedAt && mappedApplicationOK {
		respondedAt, hasRespondedAt = getNonEmptyValue(mappedApplication, "responded_at")
	}

	if hasRespondedAt {
		payload["responded_at"] = respondedAt
		if mappedApplicationOK {
			mappedApplication["responded_at"] = respondedAt
		}

		return
	}

	now := formatISOTime(time.Now().UTC())

	payload["responded_at"] = now
	if mappedApplicationOK {
		mappedApplication["responded_at"] = now
	}
}

func (t *ApplicationUpdateTransformer) updateStageIfNeeded(payload map[string]any) {
	if !t.isMeetingCreationConfirmation(payload) {
		return
	}

	mappedApplication, ok := getObject(payload, "mapped_application")
	if !ok {
		t.logger.Warn("skipping stage update: mapped_application is missing or invalid")

		return
	}

	_, hasApplicationID := parseInt64(mappedApplication["id"])
	_, hasRowID := parseInt64(mappedApplication["row_id"])
	if !hasApplicationID || !hasRowID {
		t.logger.Warn(
			"skipping stage update: application_id or row_id is missing",
			slog.Any("application_id", mappedApplication["id"]),
			slog.Any("row_id", mappedApplication["row_id"]),
		)

		return
	}
	currentStage := t.parseStage(mappedApplication["stage"])
	nextStage := currentStage + 1

	mappedApplication["stage"] = nextStage
	payload["stage"] = nextStage
}

func (t *ApplicationUpdateTransformer) enrichMeetingConfirmation(payload map[string]any) {
	if !t.isMeetingConfirmation(payload) {
		return
	}

	mappedApplication, ok := getObject(payload, "mapped_application")
	if !ok {
		t.logger.Warn("skipping meeting confirmation enrichment: mapped_application is missing or invalid")

		return
	}

	t.ensureMeetingStatus(payload, mappedApplication)
	t.ensureRespondedAt(payload, mappedApplication)
	t.ensureNextFollowUp(payload, mappedApplication)
}

func (t *ApplicationUpdateTransformer) ensureMeetingStatus(payload map[string]any, mappedApplication map[string]any) {
	if !shouldSetMeetingStatus(mappedApplication["status"]) {
		return
	}

	mappedApplication["status"] = "meeting"
	payload["status"] = "meeting"
}

func (t *ApplicationUpdateTransformer) ensureRespondedAt(payload map[string]any, mappedApplication map[string]any) {
	respondedAt, ok := getNonEmptyValue(mappedApplication, "responded_at")
	if ok {
		payload["responded_at"] = respondedAt

		return
	}

	now := formatISOTime(time.Now().UTC())

	mappedApplication["responded_at"] = now
	payload["responded_at"] = now
}

func (t *ApplicationUpdateTransformer) ensureNextFollowUp(payload map[string]any, mappedApplication map[string]any) {
	if t.emailLabel(payload) != meetingCreationLabel {
		return
	}

	if _, ok := getNonEmptyValue(mappedApplication, "next_follow_up_at"); ok {
		return
	}

	email, ok := getObject(payload, "email")
	if !ok {
		return
	}

	meetingTime, ok := t.extractMeetingDateTime(email)
	if !ok {
		t.logger.Info("no meeting datetime extracted from email", slog.Any("email_id", email["id"]))

		return
	}

	meetingISO := formatISOTime(meetingTime)

	mappedApplication["next_follow_up_at"] = meetingISO
	payload["next_follow_up_at"] = meetingISO

	t.logger.Info("enriched next_follow_up_at from ICS", slog.Any("email_id", email["id"]), slog.String("value", meetingISO))
}

func (t *ApplicationUpdateTransformer) extractMeetingDateTime(email map[string]any) (time.Time, bool) {
	icsList, ok := getArray(email, "ics_file_data_list")
	if !ok {
		icsList, ok = getArray(email, "ics_files")
		if !ok {
			return time.Time{}, false
		}
	}

	for _, item := range icsList {
		icsItem, ok := item.(map[string]any)
		if !ok {
			continue
		}

		content, _ := icsItem["content"].(string)
		meetingTime, found := t.parseDTStart(content)
		if found {
			return meetingTime, true
		}
	}

	return time.Time{}, false
}

func (t *ApplicationUpdateTransformer) parseDTStart(content string) (time.Time, bool) {
	if strings.TrimSpace(content) == "" {
		return time.Time{}, false
	}

	match := dtStartPattern.FindStringSubmatch(content)
	if match == nil {
		t.logger.Info("DTSTART not found in ICS content")

		return time.Time{}, false
	}

	tzID := match[1]
	timestamp := match[2]
	timezoneSuffix := match[3]

	parsedTime, err := time.Parse("20060102T150405", timestamp)
	if err != nil {
		t.logger.Warn("failed to parse timestamp from ICS", slog.String("timestamp", timestamp), slog.Any("error", err))

		return time.Time{}, false
	}

	if timezoneSuffix == "Z" {
		return parsedTime.UTC(), true
	}

	if tzID == "" {
		return parsedTime.UTC(), true
	}

	location, err := time.LoadLocation(tzID)
	if err != nil {
		t.logger.Warn("failed to apply timezone for DTSTART", slog.String("tzid", tzID), slog.Any("error", err))

		return parsedTime.UTC(), true
	}

	zonedTime, err := time.ParseInLocation("20060102T150405", timestamp, location)
	if err != nil {
		t.logger.Warn("failed to parse timestamp in timezone", slog.String("timestamp", timestamp), slog.String("tzid", tzID), slog.Any("error", err))

		return time.Time{}, false
	}

	return zonedTime, true
}

func (t *ApplicationUpdateTransformer) isMeetingCreationConfirmation(payload map[string]any) bool {
	label := t.emailLabel(payload)
	action := normalizedString(payload["action"])

	if label != meetingCreationLabel {
		return false
	}

	if action != "" && action != "cnfm" {
		return false
	}

	return true
}

func (t *ApplicationUpdateTransformer) isMeetingConfirmation(payload map[string]any) bool {
	label := t.emailLabel(payload)
	action := normalizedString(payload["action"])

	if _, ok := meetingConfirmationLabels[label]; !ok {
		return false
	}

	if action != "" && action != "cnfm" {
		return false
	}

	return true
}

func (t *ApplicationUpdateTransformer) isDeniedConfirmation(payload map[string]any) bool {
	label := t.emailLabel(payload)
	action := normalizedString(payload["action"])

	if label != "denied" {
		return false
	}

	if action != "" && action != "cnfm" {
		return false
	}

	return true
}

func (t *ApplicationUpdateTransformer) emailLabel(payload map[string]any) string {
	email, ok := getObject(payload, "email")
	if !ok {
		return ""
	}

	return normalizedString(email["label"])
}

func (t *ApplicationUpdateTransformer) parseStage(value any) int64 {
	stage, ok := parseInt64(value)
	if ok {
		return stage
	}

	if value != nil {
		t.logger.Warn("invalid stage value, defaulting to zero", slog.Any("stage", value))
	}

	return 0
}

func decodeJSONObject(payloadBytes []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.UseNumber()

	var payload map[string]any

	err := decoder.Decode(&payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func getObject(payload map[string]any, key string) (map[string]any, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil, false
	}

	objectValue, ok := value.(map[string]any)

	return objectValue, ok
}

func getArray(payload map[string]any, key string) ([]any, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil, false
	}

	arrayValue, ok := value.([]any)

	return arrayValue, ok
}

func getNonEmptyValue(payload map[string]any, key string) (any, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil, false
	}

	switch typedValue := value.(type) {
	case string:
		if strings.TrimSpace(typedValue) == "" {
			return nil, false
		}
	}

	return value, true
}

func parseInt64(value any) (int64, bool) {
	switch typedValue := value.(type) {
	case nil:
		return 0, false
	case int:
		return int64(typedValue), true
	case int8:
		return int64(typedValue), true
	case int16:
		return int64(typedValue), true
	case int32:
		return int64(typedValue), true
	case int64:
		return typedValue, true
	case float32:
		return int64(typedValue), true
	case float64:
		return int64(typedValue), true
	case json.Number:
		parsedValue, err := typedValue.Int64()
		if err == nil {
			return parsedValue, true
		}
	case string:
		trimmedValue := strings.TrimSpace(typedValue)
		if trimmedValue == "" {
			return 0, false
		}

		parsedValue, err := strconv.ParseInt(trimmedValue, 10, 64)
		if err == nil {
			return parsedValue, true
		}
	}

	return 0, false
}

func shouldSetMeetingStatus(value any) bool {
	if value == nil {
		return true
	}

	switch normalizedValue := normalizedString(value); normalizedValue {
	case "meeting", "offer", "denied":
		return false
	default:
		return true
	}
}

func normalizedString(value any) string {
	if value == nil {
		return ""
	}

	return strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
}

func formatISOTime(value time.Time) string {
	return value.Format("2006-01-02T15:04:05-07:00")
}
