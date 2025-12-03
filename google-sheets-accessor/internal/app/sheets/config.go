package sheets

import (
	"errors"
	"os"
	"time"
)

const (
	FirstRowIDAfterHeader = 3
	LastRow               = 10000
	RequestTimeout        = 30 * time.Second
)

type Config struct {
	CredentialsPath       string
	SpreadSheetID         string
	RequestTimeout        time.Duration
	FirstRowIDAfterHeader int
}

func LoadConfig() (*Config, error) {
	spreadsheetID := os.Getenv("SHEET_ID")
	if spreadsheetID == "" {
		return nil, errors.New("SHEET_ID is required")
	}

	credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentialsPath == "" {
		return nil, errors.New("GOOGLE_APPLICATION_CREDENTIALS is required")
	}

	config := Config{
		CredentialsPath:       credentialsPath,
		SpreadSheetID:         spreadsheetID,
		RequestTimeout:        RequestTimeout,
		FirstRowIDAfterHeader: FirstRowIDAfterHeader,
	}

	return &config, nil
}
