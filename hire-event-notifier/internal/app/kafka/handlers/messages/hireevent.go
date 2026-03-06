package messages

import (
	"encoding/json"
	"time"
)

type HireEventMessage struct {
	Email             EmailPayload      `json:"email"`
	MappedApplication MappedApplication `json:"mapped_application"`
	Action            string            `json:"action,omitempty"`
	Status            string            `json:"status,omitempty"`
	Stage             NullableInt64     `json:"stage,omitempty"`
	RespondedAt       *time.Time        `json:"responded_at,omitempty"`
	NextFollowUpAt    *time.Time        `json:"next_follow_up_at,omitempty"`
}

type EmailPayload struct {
	ID              int64           `json:"id"`
	CreatedAt       time.Time       `json:"created_at,omitempty"`
	UpdatedAt       time.Time       `json:"updated_at,omitempty"`
	InboxEmailID    *int64          `json:"inbox_email_id,omitempty"`
	Label           string          `json:"label"`
	Subject         string          `json:"subject"`
	Body            string          `json:"body"`
	Content         string          `json:"content"`
	SenderEmail     string          `json:"sender_email"`
	SenderName      string          `json:"sender_name"`
	RecipientName   string          `json:"recipient_name,omitempty"`
	RecipientEmail  string          `json:"recipient_email,omitempty"`
	ContentType     string          `json:"content_type,omitempty"`
	Meta            json.RawMessage `json:"meta,omitempty"`
	UsedForLearning string          `json:"used_for_learning,omitempty"`
	ShouldBeSent    bool            `json:"should_be_sent_to_heh"`
	IcsFiles        []IcsFileData   `json:"ics_file_data_list,omitempty"`
}

type IcsFileData struct {
	ID          int64           `json:"id"`
	EmbdLrnID   int64           `json:"embd_lrn_id"`
	Filename    string          `json:"filename"`
	ContentType string          `json:"content_type"`
	Disposition string          `json:"disposition,omitempty"`
	Method      string          `json:"method,omitempty"`
	Size        int64           `json:"size"`
	Content     string          `json:"content"`
	CreatedAt   time.Time       `json:"created_at,omitempty"`
	UpdatedAt   time.Time       `json:"updated_at,omitempty"`
	Meta        json.RawMessage `json:"meta,omitempty"`
}

type MappedApplication struct {
	ID             int64           `json:"id,omitempty"`
	RowID          NullableInt64   `json:"row_id,omitempty"`
	Company        string          `json:"company,omitempty"`
	Title          string          `json:"title,omitempty"`
	Status         string          `json:"status,omitempty"`
	Stage          NullableInt64   `json:"stage,omitempty"`
	RespondedAt    *time.Time      `json:"responded_at,omitempty"`
	NextFollowUpAt *time.Time      `json:"next_follow_up_at,omitempty"`
	Embedding      []float32       `json:"embedding,omitempty"`
	Meta           json.RawMessage `json:"meta,omitempty"`
}
