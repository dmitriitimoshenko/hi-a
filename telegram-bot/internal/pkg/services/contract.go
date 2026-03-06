package services

import (
	"context"

	"github.com/go-telegram/bot/models"
)

type botClient interface {
	SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error
}
