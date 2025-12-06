package messages

import (
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
)

/// DIFF request

type DiffRequest struct {
	SheetRange DiffRange `json:"sheet_range"`
}

type DiffRange struct {
	StartRow int64 `json:"start_row"`
	EndRow   int64 `json:"end_row"`
}

// DIFF response

type DiffResponse struct {
	Data DiffResponseData `json:"data"`
}

type DiffResponseData struct {
	RowsChecked         int64                  `json:"rows_checked"`
	RowsWithDifferences int64                  `json:"rows_with_differences"`
	Differences         []ApplicationDiffEntry `json:"differences"`
}

type ApplicationDiffEntry struct {
	RowID       int64                   `json:"row_id"`
	Company     *string                 `json:"company,omitempty"`
	RoleTitle   *string                 `json:"role_title,omitempty"`
	Differences []ApplicationDiffChange `json:"differences"`
	Errors      []string                `json:"errors"`
}

type ApplicationDiffChange struct {
	Field      string `json:"field"`
	SheetValue string `json:"sheet_value"`
	DBValue    string `json:"db_value"`
}

// FETCH response

type FetchRequest struct {
	Data FetchData `json:"data"`
}

type FetchData struct {
	ApplicationsSavedAmount int64 `json:"applications_saved_amount"`
	SalariesSavedAmount     int64 `json:"salaries_saved_amount"`
}

// LPR response

type LastProcessedRowResponse struct {
	Data LastProcessedRowData `json:"data"`
}

type LastProcessedRowData struct {
	LastProcessedRow int64 `json:"last_processed_row"`
}

// LIST request

type ListRequest struct {
	ApplicationStatusInclude []enums.ApplicationStatus `json:"application_status_include"`
	ApplicationStatusExclude []enums.ApplicationStatus `json:"application_status_exclude"`
	IsReplyEmailReceived     bool                      `json:"is_reply_email_received"`
}

type ListResponse struct {
	Data []ListApplicationData `json:"data"`
}

type ListApplicationData struct {
	ID                       int64                   `json:"id"`
	CreatedAt                time.Time               `json:"created_at"`
	UpdatedAt                time.Time               `json:"updated_at"`
	Company                  string                  `json:"company"`
	Title                    string                  `json:"title"`
	EmploymentType           enums.EmploymentType    `json:"employment_type"`
	WorkMode                 enums.WorkMode          `json:"work_mode"`
	Status                   enums.ApplicationStatus `json:"status"`
	AppliedAt                time.Time               `json:"applied_at"`
	RespondedAt              *time.Time              `json:"responded_at"`
	NextFollowUpAt           *time.Time              `json:"next_follow_up_at"`
	Stage                    *string                 `json:"stage"`
	Meta                     *map[string]string      `json:"meta"`
	Embedding                []float32               `json:"embedding"`
	RowID                    int64                   `json:"row_id"`
	AppliedEmailReceived     *time.Time              `json:"applied_email_received"`
	AppliedEmailID           *int64                  `json:"applied_email_id"`
	DeniedEmailReceived      *time.Time              `json:"denied_email_received"`
	DeniedEmailID            *int64                  `json:"denied_email_id"`
	MeetingInvEmailReceived  *time.Time              `json:"meeting_inv_email_received"`
	MeetingInvEmailID        *int64                  `json:"meeting_inv_email_id"`
	MeetingCrtEmailReceived  *time.Time              `json:"meeting_crt_email_received"`
	MeetingCrtEmailID        *int64                  `json:"meeting_crt_email_id"`
	MeetingUpdEmailReceived  *time.Time              `json:"meeting_upd_email_received"`
	MeetingUpdEmailID        *int64                  `json:"meeting_upd_email_id"`
	MeetingCnclEmailReceived *time.Time              `json:"meeting_cncl_email_received"`
	MeetingCnclEmailID       *int64                  `json:"meeting_cncl_email_id"`
	SalaryApplied            *ListSalaryData         `json:"salary_applied"`
	SalaryProposed           *ListSalaryData         `json:"salary_proposed"`
}

type ListSalaryData struct {
	ID         int64              `json:"id"`
	CreatedAt  time.Time          `json:"created_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
	AmountFrom *float64           `json:"amount_from"`
	AmountTo   *float64           `json:"amount_to"`
	Currency   string             `json:"currency"`
	Period     enums.SalaryPeriod `json:"period"`
}

// UPDATE INTERNAL request

type DiffUpdateRequest struct {
	ApplicationID    int64                     `json:"application_id"`
	ApplicationRowID int64                     `json:"application_row_id"`
	UpdateDirection  enums.DiffUpdateDirection `json:"update_direction"`
}

// CLEAN UP MEETINGS response

type CleanUpMeetingsResponse struct {
	Data CleanupMeetingsData `json:"data"`
}

type CleanupMeetingsData struct {
	RowsReset int64 `json:"rows_reset"`
}
