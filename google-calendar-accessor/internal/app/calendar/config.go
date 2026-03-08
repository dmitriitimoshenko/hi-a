package calendar

import (
	"errors"
	"os"
	"time"
)

const (
	RequestTimeout = 30 * time.Second
)

type Config struct {
	CredentialsPath string
	RequestTimeout  time.Duration
}

func LoadConfig() (*Config, error) {
	// /secrets/gca-credentials.json
	credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentialsPath == "" {
		return nil, errors.New("GOOGLE_APPLICATION_CREDENTIALS is required")
	}

	config := Config{
		CredentialsPath: credentialsPath,
		RequestTimeout:  RequestTimeout,
	}

	return &config, nil
}
