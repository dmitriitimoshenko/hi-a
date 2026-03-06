package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaMaxAttempts = 30

type Client struct {
	config  *Config
	dialer  *kafka.Dialer
	writers map[string]*kafka.Writer
	mutex   sync.Mutex
	logger  *slog.Logger
}

type Handler func(context.Context, kafka.Message) error
type Message = kafka.Message

func New(cfg *Config) (*Client, error) {
	dialer := &kafka.Dialer{
		Timeout:  cfg.DialTimeout,
		ClientID: cfg.ClientID,
	}

	client := &Client{
		config:  cfg,
		dialer:  dialer,
		writers: map[string]*kafka.Writer{},
		mutex:   sync.Mutex{},
		logger:  slog.Default(),
	}

	return client, nil
}

func (c *Client) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	writer, err := c.getWriter(topic)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now().UTC(),
	}

	err = writer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %w", topic, err)
	}

	return nil
}

func (c *Client) Consume(ctx context.Context, topic string, handler Handler) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.config.Brokers,
		GroupID:        c.config.GroupID,
		Topic:          topic,
		CommitInterval: c.config.CommitInterval,
		Dialer:         c.dialer,
		StartOffset:    kafka.FirstOffset,
		MaxAttempts:    kafkaMaxAttempts,
	})

	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			return fmt.Errorf("failed to fetch message: %w", err)
		}

		err = handler(ctx, message)
		if err != nil {
			c.logger.Error("handler failed", slog.String("topic", topic), slog.Any("error", err))

			continue
		}

		err = reader.CommitMessages(ctx, message)
		if err != nil {
			return fmt.Errorf("failed to commit message: %w", err)
		}
	}
}

func (c *Client) Close(_ context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var closeErrors []string

	for topic, writer := range c.writers {
		closeErr := writer.Close()
		if closeErr != nil {
			c.logger.Error("failed to close writer", slog.String("topic", topic), slog.Any("error", closeErr))

			closeErrors = append(closeErrors, fmt.Sprintf("%s: %v", topic, closeErr))
		}
	}

	if len(closeErrors) > 0 {
		return fmt.Errorf("writer close errors: %s", strings.Join(closeErrors, "; "))
	}

	return nil
}

func (c *Client) getWriter(topic string) (*kafka.Writer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	writer, ok := c.writers[topic]
	if ok {
		return writer, nil
	}

	newWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      c.config.Brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		MaxAttempts:  kafkaMaxAttempts,
		RequiredAcks: int(kafka.RequireAll),
		Dialer:       c.dialer,
	})

	c.writers[topic] = newWriter

	return newWriter, nil
}
