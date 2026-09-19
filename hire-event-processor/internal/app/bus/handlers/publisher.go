package handlers

import "context"

type publisher interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}
