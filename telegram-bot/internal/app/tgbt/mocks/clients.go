package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

type RedisClientMock struct {
	mock.Mock
}

func (m *RedisClientMock) Set(ctx context.Context, key string, value string, expiration time.Duration) (bool, error) {
	args := m.Called(ctx, key, value, expiration)

	return args.Bool(0), args.Error(1)
}

func (m *RedisClientMock) Get(ctx context.Context, key string) (string, bool, error) {
	args := m.Called(ctx, key)

	return args.String(0), args.Bool(1), args.Error(2)
}

func (m *RedisClientMock) Delete(ctx context.Context, key string) (int64, error) {
	args := m.Called(ctx, key)

	var deleted int64
	if val, ok := args.Get(0).(int64); ok {
		deleted = val
	}

	return deleted, args.Error(1)
}

type KafkaClientMock struct {
	mock.Mock
}

func (m *KafkaClientMock) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	args := m.Called(ctx, topic, key, value)

	return args.Error(0)
}
