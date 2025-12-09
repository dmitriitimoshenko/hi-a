package services

import (
	"context"

	"github.com/go-telegram/bot/models"
)

type bot interface {
	SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error
}
