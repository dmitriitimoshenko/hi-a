package calendar

import (
	"context"
	"fmt"
	"os"

	gcalendar "google.golang.org/api/calendar/v3"
	"google.golang.org/api/option"
)

type Client struct {
	service *gcalendar.Service
	config  *Config
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	credentials, err := os.ReadFile(cfg.CredentialsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read google credentials: %w", err)
	}

	service, err := gcalendar.NewService(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create sheets client: %w", err)
	}

	client := &Client{
		service: service,
		config:  cfg,
	}

	return client, nil
}
