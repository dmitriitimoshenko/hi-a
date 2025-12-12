package dto

import (
	"encoding/json"
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
)

type NewMappedEmailMessageContent struct {
	Label             enums.EmailLabel `json:"label"`
	Email             EmailPayload     `json:"email"`
	MappedApplication ApplicationData  `json:"mapped_application"`
	EmailID           int64            `json:"email_id"`
}

type ApplicationData struct {
	ID             int64      `json:"id,omitempty"`
	RowID          int64      `json:"row_id,omitempty"`
	Title          string     `json:"title,omitempty"`
	Company        string     `json:"company,omitempty"`
	Status         string     `json:"status,omitempty"`
	Stage          *int64     `json:"stage,omitempty"`
	RespondedAt    *time.Time `json:"responded_at,omitempty"`
	NextFollowUpAt *time.Time `json:"next_follow_up_at,omitempty"`
	Embedding      []float32  `json:"embedding,omitempty"` // gonna be only NIL at this point
}

type EmailPayload struct {
	ID                int64            `json:"id"`
	CreatedAt         time.Time        `json:"created_at"`
	UpdatedAt         time.Time        `json:"updated_at"`
	InboxEmailID      *int64           `json:"inbox_email_id,omitempty"`
	Label             enums.EmailLabel `json:"label"`
	Subject           string           `json:"subject"`
	Body              string           `json:"body"`
	Content           string           `json:"content"`
	SenderEmail       string           `json:"sender_email"`
	SenderName        string           `json:"sender_name"`
	RecipientName     string           `json:"recipient_name"`
	RecipientEmail    string           `json:"recipient_email"`
	ContentType       string           `json:"content_type"`
	Meta              json.RawMessage  `json:"meta"`
	UsedForLearning   *time.Time       `json:"used_for_learning,omitempty"`
	ShouldBeSentToHeh bool             `json:"should_be_sent_to_heh"`
	ICSFileDataList   []ICSFileData    `json:"ics_file_data_list,omitempty"`
}

type ICSFileData struct {
	ID          int64     `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	EmbdLrnID   int64     `json:"embd_lrn_id"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Disposition string    `json:"disposition"`
	Method      string    `json:"method"`
	Size        int64     `json:"size"`
	Content     string    `json:"content"`
}
