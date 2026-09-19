package bus

import (
	"errors"
	"os"
	"strconv"
	"time"
)

const (
	defaultDB          = 2
	defaultMaxLen      = 10000
	defaultBatchCount  = 10
	defaultBlock       = 5 * time.Second
	defaultDialTimeout = 10 * time.Second
)

// Config describes how a service talks to the Redis Streams bus.
type Config struct {
	RedisURL      string
	DB            int
	GroupID       string
	ConsumerID    string
	MaxLen        int64
	BatchCount    int64
	Block         time.Duration
	DialTimeout   time.Duration
	StartAtOldest bool
}

func LoadConfig() (*Config, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return nil, errors.New("REDIS_URL is required")
	}

	db, err := intFromEnv("STREAM_DB", defaultDB)
	if err != nil {
		return nil, err
	}

	maxLen, err := intFromEnv("STREAM_MAX_LEN", defaultMaxLen)
	if err != nil {
		return nil, err
	}

	busConfig := Config{
		RedisURL:      redisURL,
		DB:            db,
		GroupID:       os.Getenv("STREAM_GROUP"),
		ConsumerID:    os.Getenv("STREAM_CONSUMER_ID"),
		MaxLen:        int64(maxLen),
		BatchCount:    defaultBatchCount,
		Block:         defaultBlock,
		DialTimeout:   defaultDialTimeout,
		StartAtOldest: os.Getenv("STREAM_START_AT_OLDEST") == "true",
	}

	return &busConfig, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New(name + " must be an integer")
	}

	return value, nil
}
