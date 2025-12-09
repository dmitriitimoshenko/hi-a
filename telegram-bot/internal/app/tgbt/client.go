package tgbt

import (
	"context"
	"fmt"

	tgbot "github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type Client struct {
	cfg Config
	bot *tgbot.Bot
}

func NewClient(cfg Config, bot *tgbot.Bot) *Client {
	return &Client{
		cfg: cfg,
		bot: bot,
	}
}

func (c *Client) SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error {
	params := &tgbot.SendMessageParams{
		ChatID:    c.cfg.ChatID,
		Text:      message,
		ParseMode: models.ParseModeHTML,
	}

	if keyboard != nil {
		params.ReplyMarkup = keyboard
	}

	if _, err := c.bot.SendMessage(ctx, params); err != nil {
		return fmt.Errorf("failed to send telegram message: %w", err)
	}

	return nil
}
