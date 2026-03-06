package dto

import (
	"time"
)

type DiffMessageContent struct {
	Company      string
	Role         string
	Differencies []ApplicationDifference
	Errors       []string
	DetectedAt   time.Time
	RowID        int64
}

type ApplicationDifference struct {
	Field      string      `json:"field"`
	DBValue    interface{} `json:"db_value"`
	SheetValue interface{} `json:"sheet_value"`
}
