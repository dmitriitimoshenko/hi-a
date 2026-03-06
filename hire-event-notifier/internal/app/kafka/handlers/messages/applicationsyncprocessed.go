package messages

type ApplicationDifference struct {
	Field      string `json:"field"`
	DBValue    any    `json:"db_value"`
	SheetValue any    `json:"sheet_value"`
}

type ApplicationSyncProcessedMessage struct {
	ApplicationID NullableInt64           `json:"application_id,omitempty"`
	RowID         NullableInt64           `json:"row_id,omitempty"`
	Company       string                  `json:"company"`
	RoleTitle     string                  `json:"role_title"`
	Differences   []ApplicationDifference `json:"differences"`
	Errors        []string                `json:"errors"`
	DetectedAt    string                  `json:"detected_at"`
}

type NotificationSyncMessage struct {
	EventID     string                  `json:"event_id"`
	Type        string                  `json:"type"`
	RowID       int64                   `json:"row_id"`
	Company     string                  `json:"company"`
	RoleTitle   string                  `json:"role_title"`
	Differences []ApplicationDifference `json:"differences"`
	Errors      []string                `json:"errors"`
	DetectedAt  string                  `json:"detected_at"`
}
