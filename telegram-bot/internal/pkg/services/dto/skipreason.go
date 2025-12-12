package dto

import (
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
)

type FeedbackEvent struct {
	EmailID       string                       `json:"email_id"`
	ApplicationID int64                        `json:"application_id"`
	Reason        enums.MappingSkipOption      `json:"reason"`
	UserEmail     string                       `json:"user_email"`
	TelegramUser  TelegramUser                 `json:"telegram_user"`
	Payload       NewMappedEmailMessageContent `json:"payload"`
	Source        string                       `json:"source"`
	CreatedAt     time.Time                    `json:"created_at"`
}

type TelegramUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}
