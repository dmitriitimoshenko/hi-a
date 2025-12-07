package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	client *goredis.Client
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	options := &goredis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:       cfg.DB,
		Password: cfg.Password,
	}

	redisClient := goredis.NewClient(options)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	client := &Client{
		client: redisClient,
	}

	return client, nil
}

func (c *Client) Set(ctx context.Context, key string, value string, expiration time.Duration) (bool, error) {
	result, err := c.client.Set(ctx, key, value, expiration).Result()
	if err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return result == "OK", nil
}

func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	value, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return "", false, nil
		}

		return "", false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	return value, true, nil
}

func (c *Client) Delete(ctx context.Context, key string) (int64, error) {
	deleted, err := c.client.Del(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	return deleted, nil
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check key %s existence: %w", key, err)
	}

	return exists > 0, nil
}

func (c *Client) FlushAll(ctx context.Context) error {
	if err := c.client.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("failed to flush redis: %w", err)
	}

	return nil
}

func (c *Client) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("failed to close redis client: %w", err)
	}

	return nil
}
