package messages

import (
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
)

type NotificationMessage struct {
	Email             dto.EmailPayload    `json:"email"`
	MappedApplication dto.ApplicationData `json:"mapped_application"`
	Action            string              `json:"action,omitempty"`
	Status            string              `json:"status,omitempty"`
	Stage             *int64              `json:"stage,omitempty"`
	RespondedAt       *time.Time          `json:"responded_at,omitempty"`
	NextFollowUpAt    *time.Time          `json:"next_follow_up_at,omitempty"`
}
