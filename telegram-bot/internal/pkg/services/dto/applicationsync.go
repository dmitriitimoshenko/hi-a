package dto

type ApplicationSync struct {
	EventID     string                  `json:"event_id"`
	Type        string                  `json:"type"`
	RowID       int64                   `json:"row_id"`
	Company     string                  `json:"company"`
	RoleTitle   string                  `json:"role_title"`
	Differences []ApplicationDifference `json:"differences"`
	Errors      []string                `json:"errors"`
	DetectedAt  string                  `json:"detected_at"`
}
