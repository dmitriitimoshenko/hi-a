package sheets

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/api/option"
	sheetsapi "google.golang.org/api/sheets/v4"
)

type Client struct {
	service *sheetsapi.Service
	config  *Config
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	credentials, err := os.ReadFile(cfg.CredentialsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read google credentials: %w", err)
	}

	service, err := sheetsapi.NewService(ctx, option.WithCredentialsJSON(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create sheets client: %w", err)
	}

	client := &Client{
		service: service,
		config:  cfg,
	}

	return client, nil
}

func (c *Client) Read(ctx context.Context, sheetRange string) ([][]string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	response, err := c.service.Spreadsheets.Values.
		Get(c.config.SpreadSheetID, sheetRange).
		ValueRenderOption("UNFORMATTED_VALUE").
		Context(requestCtx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("failed to read range: %w", err)
	}

	data := make([][]string, len(response.Values))

	for i, row := range response.Values {
		data[i] = make([]string, len(row))
		for j, v := range row {
			if s, ok := v.(string); ok {
				data[i][j] = s
				continue
			}

			if v == nil {
				data[i][j] = ""
			}

			data[i][j] = fmt.Sprint(v)
		}
	}

	return data, nil
}

func (c *Client) Write(ctx context.Context, value string, ceil string) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()

	valueRange := &sheetsapi.ValueRange{
		Values: [][]any{
			{value},
		},
	}

	_, err := c.service.Spreadsheets.Values.
		Update(c.config.SpreadSheetID, ceil, valueRange).
		ValueInputOption("USER_ENTERED").
		Context(requestCtx).
		Do()
	if err != nil {
		return fmt.Errorf("failed to write value to %s: %w", ceil, err)
	}

	return nil
}
