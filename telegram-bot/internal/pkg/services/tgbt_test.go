package services_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/mocks"
	"github.com/go-telegram/bot/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelegramBotServiceBuildApplicationDiffMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  dto.DiffMessageContent
		expected string
	}{
		{
			name: "with differences errors and timestamp",
			content: dto.DiffMessageContent{
				Company: "Acme",
				Role:    "Engineer",
				RowID:   42,
				Differencies: []dto.ApplicationDifference{
					{Field: "title", DBValue: "Dev", SheetValue: "Lead"},
					{Field: "status", DBValue: "pending", SheetValue: "responded"},
				},
				Errors: []string{"first error", "second error"},
				DetectedAt: time.Date(
					2024,
					time.April,
					5,
					10,
					11,
					12,
					0,
					time.UTC,
				),
			},
			expected: func() string {
				msg := fmt.Sprintf(
					"In application (row %d) for role <b>%s</b> in company <b>%s</b> we noticed the following changes:\n",
					42,
					"Engineer",
					"Acme",
				)
				msg = fmt.Sprintf("%s• No detailed field differences provided", msg)
				msg = fmt.Sprintf("%s• <code>%s</code>: %v → %v\n", msg, "title", "Dev", "Lead")
				msg = fmt.Sprintf("%s• <code>%s</code>: %v → %v\n", msg, "status", "pending", "responded")
				msg = fmt.Sprintf("%s\n", msg)
				msg = fmt.Sprintf("%sHowever, we encountered some issues while processing the application:\n", msg)
				msg = fmt.Sprintf("%s• %s\n", msg, "first error")
				msg = fmt.Sprintf("%s• %s\n", msg, "second error")
				msg = fmt.Sprintf("%s\n", msg)
				msg = fmt.Sprintf("%sDetected at: %s\n", msg, "2024-04-05 10:11:12")
				msg = fmt.Sprintf("%s\n", msg)

				return fmt.Sprintf("%sApply updates or skip.", msg)
			}(),
		},
		{
			name: "without differences or errors",
			content: dto.DiffMessageContent{
				Company: "Beta",
				Role:    "Lead",
				RowID:   7,
			},
			expected: func() string {
				msg := fmt.Sprintf(
					"In application (row %d) for role <b>%s</b> in company <b>%s</b> we noticed the following changes:\n",
					7,
					"Lead",
					"Beta",
				)
				msg = fmt.Sprintf("%s• No detailed field differences provided", msg)

				return fmt.Sprintf("%sApply updates or skip.", msg)
			}(),
		},
		{
			name: "escapes html and nil values",
			content: dto.DiffMessageContent{
				Company: "A < B",
				Role:    "Engineer <Lead>",
				RowID:   9,
				Differencies: []dto.ApplicationDifference{
					{Field: "stage<code>", DBValue: nil, SheetValue: "<updated>"},
				},
				Errors: []string{"broken <tag>"},
			},
			expected: func() string {
				msg := "In application (row 9) for role <b>Engineer &lt;Lead&gt;</b> in company <b>A &lt; B</b> we noticed the following changes:\n"
				msg = fmt.Sprintf("%s• No detailed field differences provided", msg)
				msg = fmt.Sprintf("%s• <code>%s</code>: %s → %s\n", msg, "stage&lt;code&gt;", "nil", "&lt;updated&gt;")
				msg = fmt.Sprintf("%s\n", msg)
				msg = fmt.Sprintf("%sHowever, we encountered some issues while processing the application:\n", msg)
				msg = fmt.Sprintf("%s• %s\n", msg, "broken &lt;tag&gt;")
				msg = fmt.Sprintf("%s\n", msg)

				return fmt.Sprintf("%sApply updates or skip.", msg)
			}(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service := services.NewTelegramBotService(nil)

			message := service.BuildApplicationDiffMessage(tt.content)

			assert.Equal(t, tt.expected, message)
		})
	}
}

func TestTelegramBotServiceBuildApplicationDiffKeyboard(t *testing.T) {
	t.Parallel()

	service := services.NewTelegramBotService(nil)

	keyboard := service.BuildApplicationDiffKeyboard("evt-123")

	require.NotNil(t, keyboard)
	require.Len(t, keyboard.InlineKeyboard, 1)

	row := keyboard.InlineKeyboard[0]

	require.Len(t, row, 3)
	assert.Equal(t, "Apply Google Sheet", row[0].Text)
	assert.Equal(t, "application_diff:aplsh:evt-123", row[0].CallbackData)
	assert.Equal(t, "Apply internal", row[1].Text)
	assert.Equal(t, "application_diff:aplin:evt-123", row[1].CallbackData)
	assert.Equal(t, "Skip", row[2].Text)
	assert.Equal(t, "application_diff:skp:evt-123", row[2].CallbackData)
}

func TestTelegramBotServiceSendMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	keyboard := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{{{Text: "ok"}}},
	}

	tests := []struct {
		name        string
		setupMock   func(bot *mocks.BotMock)
		expectError string
	}{
		{
			name: "success",
			setupMock: func(bot *mocks.BotMock) {
				bot.On("SendMessage", ctx, "message", keyboard).Return(nil).Once()
			},
		},
		{
			name: "failure",
			setupMock: func(bot *mocks.BotMock) {
				bot.On("SendMessage", ctx, "message", keyboard).Return(fmt.Errorf("send failed")).Once()
			},
			expectError: "failed to send telegram message: send failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			botMock := &mocks.BotMock{}
			tt.setupMock(botMock)

			service := services.NewTelegramBotService(botMock)

			err := service.SendMessage(ctx, "message", keyboard)

			if tt.expectError != "" {
				require.EqualError(t, err, tt.expectError)
			} else {
				require.NoError(t, err)
			}

			botMock.AssertExpectations(t)
		})
	}
}

func TestTelegramBotServiceBuildNewMappedEmailMessage(t *testing.T) {
	t.Parallel()

	email := dto.EmailPayload{
		SenderName:  "Alice",
		SenderEmail: "alice@example.com",
	}
	mapped := dto.ApplicationData{
		Title:   "Backend Engineer",
		Company: "Acme",
	}

	labelTemplates := map[enums.EmailLabel]string{
		enums.EmailLabelApplied:     "You received an email from %s (%s), that tells you have applied on position <b>%s</b> at <b>%s</b>\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelDenied:      "You received an email from %s (%s), that tells that you application on role <b>%s</b> at <b>%s</b> was denied\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelMeetingInv:  "You received an email from %s (%s), that tells that you were invited to a meeting for role <b>%s</b> at <b>%s</b>\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelMeetingCrt:  "You received an email from %s (%s), that tells that a meeting was scheduled to talk with you about role <b>%s</b> at <b>%s</b>\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelMeetingUpd:  "You received an email from %s (%s), that tells that a meeting was RE-scheduled to talk with you about role <b>%s</b> at <b>%s</b>\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelMeetingCncl: "You received an email from %s (%s), that tells that a meeting about role <b>%s</b> at <b>%s</b> was cancelled\n\nPlease confirm if I understood everything correctly or let me skip this case",
		enums.EmailLabelOffer:       "Waaait... Is it true?? It seems you received an email from %s (%s), that tells you got an OFFER 🎉🎉🎉 for role <b>%s</b> at <b>%s</b>!\n\nPlease confirm if I understood everything correctly or let me skip this case",
	}
	defaultTemplate := "You received an email from %s (%s) about role <b>%s</b> at <b>%s</b>"

	tests := []struct {
		name  string
		label enums.EmailLabel
	}{
		{name: "applied", label: enums.EmailLabelApplied},
		{name: "denied", label: enums.EmailLabelDenied},
		{name: "meeting invitation", label: enums.EmailLabelMeetingInv},
		{name: "meeting created", label: enums.EmailLabelMeetingCrt},
		{name: "meeting updated", label: enums.EmailLabelMeetingUpd},
		{name: "meeting canceled", label: enums.EmailLabelMeetingCncl},
		{name: "offer", label: enums.EmailLabelOffer},
		{name: "default", label: enums.EmailLabelPending},
	}

	service := services.NewTelegramBotService(nil)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			content := dto.NewMappedEmailMessageContent{
				Label:             tt.label,
				Email:             email,
				MappedApplication: mapped,
			}

			message := service.BuildNewMappedEmailMessage(content)

			if template, ok := labelTemplates[tt.label]; ok {
				require.Equal(t, fmt.Sprintf(template, email.SenderName, email.SenderEmail, mapped.Title, mapped.Company), message)
			} else {
				require.Equal(t, fmt.Sprintf(defaultTemplate, email.SenderName, email.SenderEmail, mapped.Title, mapped.Company), message)
			}
		})
	}
}

func TestTelegramBotServiceBuildNewMappedEmailKeyboard(t *testing.T) {
	t.Parallel()

	service := services.NewTelegramBotService(nil)

	prefix := enums.EmailLabelApplied
	emailID := int64(12)

	tests := []struct {
		name            string
		hideConfirm     bool
		hideSkip        bool
		expectedButtons []models.InlineKeyboardButton
	}{
		{
			name: "all actions",
			expectedButtons: []models.InlineKeyboardButton{
				{
					Text:         "Confirm",
					CallbackData: "applied:cnfm:12",
				},
				{
					Text:         "Details",
					CallbackData: "applied:dtls:12",
				},
				{
					Text:         "Skip",
					CallbackData: "applied:skp:12",
				},
			},
		},
		{
			name:        "skip confirm",
			hideConfirm: true,
			expectedButtons: []models.InlineKeyboardButton{
				{
					Text:         "Details",
					CallbackData: "applied:dtls:12",
				},
				{
					Text:         "Skip",
					CallbackData: "applied:skp:12",
				},
			},
		},
		{
			name:     "skip skip",
			hideSkip: true,
			expectedButtons: []models.InlineKeyboardButton{
				{
					Text:         "Confirm",
					CallbackData: "applied:cnfm:12",
				},
				{
					Text:         "Details",
					CallbackData: "applied:dtls:12",
				},
			},
		},
		{
			name:        "only details",
			hideConfirm: true,
			hideSkip:    true,
			expectedButtons: []models.InlineKeyboardButton{
				{
					Text:         "Details",
					CallbackData: "applied:dtls:12",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keyboard := service.BuildNewMappedEmailKeyboard(prefix, emailID, tt.hideConfirm, tt.hideSkip)

			require.NotNil(t, keyboard)
			require.Len(t, keyboard.InlineKeyboard, 1)

			row := keyboard.InlineKeyboard[0]

			assert.Equal(t, tt.expectedButtons, row)
		})
	}
}
