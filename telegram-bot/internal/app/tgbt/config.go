package tgbt

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ChatID   int64
	BotToken string
}

func LoadConfig() (*Config, error) {
	chatIDStr := os.Getenv("TG_ID")
	if chatIDStr == "" {
		return nil, errors.New("LoadBotConfig: TG_ID isn't set")
	}

	botToken := os.Getenv("BOT_TOKEN")
	if botToken == "" {
		return nil, errors.New("LoadBotConfig: BOT_TOKEN isn't set")
	}

	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("LoadBotConfig: failed to parse TG_ID: %w", err)
	}

	return &Config{
		ChatID:   chatID,
		BotToken: botToken,
	}, nil
}
