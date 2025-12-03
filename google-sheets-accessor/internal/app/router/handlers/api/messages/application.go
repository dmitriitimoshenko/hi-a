package messages

/// DIFF request

type DiffRequest struct {
	ID         string    `json:"id"`
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
	Field      string      `json:"field"`
	SheetValue interface{} `json:"sheet_value"`
	DBValue    interface{} `json:"db_value"`
	Message    string      `json:"message"`
}

// FETCH response

type FetchRequest struct {
	Data FetchData
}

type FetchData struct {
	ApplicationsSavedAmount int64 `json:"applications_saved_amount"`
	SalariesSavedAmount     int64 `json:"salaries_saved_amount"`
}
