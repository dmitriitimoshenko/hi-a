package applicationupdateprocessed

import (
	"encoding/json"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
)

type ApplicationUpdatePayload struct {
	Action            string             `json:"action,omitempty"`
	Email             EmailPayload       `json:"email"`
	MappedApplication ApplicationPayload `json:"mapped_application"`
	Status            string             `json:"status,omitempty"`
	RespondedAt       *time.Time         `json:"responded_at,omitempty"`
	NextFollowUpAt    *time.Time         `json:"next_follow_up_at,omitempty"`
	Stage             NullableInt64      `json:"stage,omitempty"`
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
	ID          int64     `json:"id"`
	EmbdLrnID   int64     `json:"embd_lrn_id"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	Disposition string    `json:"disposition,omitempty"`
	Method      string    `json:"method,omitempty"`
	Size        int64     `json:"size"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

type ApplicationPayload struct {
	ID             int64                   `json:"id"`
	RowID          int64                   `json:"row_id"`
	Company        string                  `json:"company"`
	Title          string                  `json:"title"`
	Status         enums.ApplicationStatus `json:"status"`
	RespondedAt    *time.Time              `json:"responded_at,omitempty"`
	NextFollowUpAt *time.Time              `json:"next_follow_up_at,omitempty"`
	Stage          NullableInt64           `json:"stage"`
	Embedding      []float32               `json:"embedding,omitempty"`
}

type Salary struct {
	ID         int64              `json:"id"`
	CreatedAt  time.Time          `json:"created_at,omitempty"`
	UpdatedAt  time.Time          `json:"updated_at,omitempty"`
	AmountFrom *float64           `json:"amount_from,omitempty"`
	AmountTo   *float64           `json:"amount_to,omitempty"`
	Currency   string             `json:"currency"`
	Period     enums.SalaryPeriod `json:"period"`
}
