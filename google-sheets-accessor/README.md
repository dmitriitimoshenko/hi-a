# Google Sheets Accessor (Go)

Go service scaffolded following the Golang project layout. It consumes Kafka events, forwards them to Google Sheets, and republishes processed messages.

## Layout

- `cmd/google-sheets-accessor/`: service entrypoint.
- `internal/`: application wiring and configuration.
- `pkg/kafka/`: reusable Kafka producer/consumer wrapper.
- `pkg/sheets/`: Google Sheets client helpers.
- `configs/`: example configuration values.

## Quick start

1. Set environment variables:
   - `KAFKA_BROKERS` (e.g., `localhost:9092`)
   - `KAFKA_GROUP_ID`, `KAFKA_CONSUME_TOPIC`, `KAFKA_PUBLISH_TOPIC`, `KAFKA_CLIENT_ID`
   - `SPREADSHEET_ID`, `GOOGLE_CREDENTIALS_PATH`, optional `GOOGLE_SHEET_RANGE`
2. Build and run:
   ```bash
   cd google-sheets-accessor
   go run ./cmd/google-sheets-accessor
   ```

## Notes

- Go version target: 1.25.
- Dependencies are declared in `go.mod`; run `go mod tidy` to download them.
