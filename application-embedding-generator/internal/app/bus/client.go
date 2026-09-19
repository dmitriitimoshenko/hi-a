package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	messageFieldKey   = "key"
	messageFieldValue = "value"

	pendingStartID = "0"
	newMessagesID  = ">"
)

// Message is the transport-agnostic envelope handed to handlers.
type Message struct {
	Topic string
	Key   []byte
	Value []byte
}

type Handler func(context.Context, Message) error

type Client struct {
	config *Config
	redis  *redis.Client
	logger *slog.Logger
}

func New(cfg *Config) (*Client, error) {
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse REDIS_URL: %w", err)
	}

	options.DB = cfg.DB
	options.DialTimeout = cfg.DialTimeout

	consumerID := cfg.ConsumerID
	if consumerID == "" {
		hostname, hostErr := os.Hostname()
		if hostErr != nil {
			return nil, fmt.Errorf("failed to resolve consumer id: %w", hostErr)
		}

		consumerID = hostname
	}

	cfg.ConsumerID = consumerID

	client := &Client{
		config: cfg,
		redis:  redis.NewClient(options),
		logger: slog.Default(),
	}

	return client, nil
}

func (c *Client) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	args := &redis.XAddArgs{
		Stream: topic,
		MaxLen: c.config.MaxLen,
		Approx: true,
		Values: map[string]interface{}{
			messageFieldKey:   key,
			messageFieldValue: value,
		},
	}

	err := c.redis.XAdd(ctx, args).Err()
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %w", topic, err)
	}

	return nil
}

func (c *Client) Consume(ctx context.Context, topic string, handler Handler) error {
	err := c.ensureGroup(ctx, topic)
	if err != nil {
		return err
	}

	// Entries already delivered to this consumer but never acknowledged are
	// replayed first, so a crash mid-handler does not lose the message.
	readID := pendingStartID

	for {
		streams, readErr := c.read(ctx, topic, readID)
		if readErr != nil {
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				return nil
			}

			if errors.Is(readErr, redis.Nil) {
				readID = newMessagesID

				continue
			}

			return fmt.Errorf("failed to read from %s: %w", topic, readErr)
		}

		messages := collectMessages(streams)

		// While replaying the pending list, page forward by the last seen ID.
		// Entries whose handler fails are left unacknowledged and skipped for
		// this run instead of being re-read in a tight loop; they come back on
		// the next restart.
		if readID != newMessagesID {
			if len(messages) == 0 {
				readID = newMessagesID
			} else {
				readID = messages[len(messages)-1].ID
			}
		}

		c.handleBatch(ctx, topic, messages, handler)
	}
}

func (c *Client) Close(_ context.Context) error {
	err := c.redis.Close()
	if err != nil {
		return fmt.Errorf("failed to close redis client: %w", err)
	}

	return nil
}

func (c *Client) ensureGroup(ctx context.Context, topic string) error {
	startID := "$"
	if c.config.StartAtOldest {
		startID = pendingStartID
	}

	err := c.redis.XGroupCreateMkStream(ctx, topic, c.config.GroupID, startID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group for %s: %w", topic, err)
	}

	return nil
}

func (c *Client) read(ctx context.Context, topic string, readID string) ([]redis.XStream, error) {
	args := &redis.XReadGroupArgs{
		Group:    c.config.GroupID,
		Consumer: c.config.ConsumerID,
		Streams:  []string{topic, readID},
		Count:    c.config.BatchCount,
		Block:    c.config.Block,
	}

	streams, err := c.redis.XReadGroup(ctx, args).Result()

	return streams, err
}

func (c *Client) handleBatch(ctx context.Context, topic string, messages []redis.XMessage, handler Handler) {
	for _, entry := range messages {
		message := Message{
			Topic: topic,
			Key:   fieldBytes(entry.Values, messageFieldKey),
			Value: fieldBytes(entry.Values, messageFieldValue),
		}

		err := handler(ctx, message)
		if err != nil {
			// Left unacknowledged on purpose: the entry stays in the pending
			// list and is retried when this consumer restarts.
			c.logger.Error("handler failed", slog.String("topic", topic), slog.Any("error", err))

			continue
		}

		ackErr := c.redis.XAck(ctx, topic, c.config.GroupID, entry.ID).Err()
		if ackErr != nil {
			c.logger.Error("failed to acknowledge message", slog.String("topic", topic), slog.Any("error", ackErr))
		}
	}
}

func collectMessages(streams []redis.XStream) []redis.XMessage {
	var messages []redis.XMessage

	for _, stream := range streams {
		messages = append(messages, stream.Messages...)
	}

	return messages
}

func fieldBytes(values map[string]interface{}, field string) []byte {
	raw, ok := values[field]
	if !ok {
		return nil
	}

	text, ok := raw.(string)
	if !ok {
		return nil
	}

	return []byte(text)
}
