package handlers

import (
	"context"
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	"github.com/go-telegram/bot/models"
)

type tgbtService interface {
	SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error

	BuildApplicationDiffMessage(diffMessageContent dto.DiffMessageContent) string
	BuildApplicationDiffKeyboard(eventID string) *models.InlineKeyboardMarkup

	BuildNewMappedEmailMessage(newMappedEmailMessage dto.NewMappedEmailMessageContent) string
	BuildNewMappedEmailKeyboard(
		prefix enums.EmailLabel,
		emailID int64,
		shouldHideConfirm bool,
		shouldHideSkip bool,
	) *models.InlineKeyboardMarkup
}

type redisClient interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) (bool, error)
}
