package services

import (
	"context"
	"fmt"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/tgbt"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	"github.com/go-telegram/bot/models"
)

type TelegramBotService struct {
	botClient *tgbt.Client
}

func NewTelegramBotService(botClient *tgbt.Client) *TelegramBotService {
	return &TelegramBotService{
		botClient: botClient,
	}
}

func (s *TelegramBotService) BuildApplicationDiffMessage(diffMessageContent dto.DiffMessageContent) string {
	diffMessage := fmt.Sprintf(
		"In application (row %d) for role <b>%s</b> in company <b>%s</b> we noticed the following changes:\n",
		diffMessageContent.RowID,
		diffMessageContent.Role,
		diffMessageContent.Company,
	)

	diffMessage = fmt.Sprintf("%s• No detailed field differences provided", diffMessage)
	if len(diffMessageContent.Differencies) > 0 {
		for _, diff := range diffMessageContent.Differencies {
			diffLine := fmt.Sprintf("• <code>%s</code>: %v → %v", diff.Field, diff.DBValue, diff.SheetValue)
			diffMessage = fmt.Sprintf("%s%s\n", diffMessage, diffLine)
		}
		diffMessage = fmt.Sprintf("%s\n", diffMessage)
	}

	if len(diffMessageContent.Errors) > 0 {
		diffMessage = fmt.Sprintf("%sHowever, we encountered some issues while processing the application:\n", diffMessage)
		for _, err := range diffMessageContent.Errors {
			errorLine := fmt.Sprintf("• %s", err)
			diffMessage = fmt.Sprintf("%s%s\n", diffMessage, errorLine)
		}
		diffMessage = fmt.Sprintf("%s\n", diffMessage)
	}

	if !diffMessageContent.DetectedAt.IsZero() {
		diffMessage = fmt.Sprintf("%sDetected at: %s\n", diffMessage, diffMessageContent.DetectedAt.Format("2006-01-02 15:04:05"))
		diffMessage = fmt.Sprintf("%s\n", diffMessage)
	}

	diffMessage = fmt.Sprintf("%sApply updates or skip.", diffMessage)

	return diffMessage
}

func (s *TelegramBotService) BuildApplicationDiffKeyboard(eventID string) *models.InlineKeyboardMarkup {
	applySheetButton := models.InlineKeyboardButton{
		Text:         "Apply Google Sheet",
		CallbackData: fmt.Sprintf("%s:aplsh:%s", enums.CallbackPrefixApplicationDiff, eventID),
	}
	applyInternalButton := models.InlineKeyboardButton{
		Text:         "Apply internal",
		CallbackData: fmt.Sprintf("%s:aplin:%s", enums.CallbackPrefixApplicationDiff, eventID),
	}
	skipButton := models.InlineKeyboardButton{
		Text:         "Skip",
		CallbackData: fmt.Sprintf("%s:skp:%s", enums.CallbackPrefixApplicationDiff, eventID),
	}

	keyboard := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{applySheetButton, applyInternalButton, skipButton},
		},
	}

	return keyboard
}

func (s *TelegramBotService) SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error {
	if err := s.botClient.SendMessage(ctx, message, keyboard); err != nil {
		return fmt.Errorf("failed to send telegram message: %w", err)
	}

	return nil
}
