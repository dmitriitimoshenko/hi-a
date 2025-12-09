package handlers

import (
	"context"
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	"github.com/go-telegram/bot/models"
)

type tgbtService interface {
	BuildApplicationDiffMessage(diffMessageContent dto.DiffMessageContent) string
	BuildApplicationDiffKeyboard(eventID string) *models.InlineKeyboardMarkup
	SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error
}

type redisClient interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) (bool, error)
}
