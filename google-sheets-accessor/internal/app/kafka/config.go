package kafka

import (
	"errors"
	"os"
	"time"
)

type Config struct {
	Broker         string
	GroupID        string
	ClientID       string
	DialTimeout    time.Duration
	CommitInterval time.Duration
}

func LoadConfig() (*Config, error) {
	kafkaServer := os.Getenv("KAFKA_SERVER")
	if kafkaServer == "" {
		return nil, errors.New("KAFKA_SERVER is required")
	}

	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	clientID := os.Getenv("KAFKA_CLIENT_ID")

	dialTimeout := 10 * time.Second
	commitInterval := 2 * time.Second

	kafkaConfig := Config{
		Broker:         kafkaServer,
		GroupID:        groupID,
		ClientID:       clientID,
		DialTimeout:    dialTimeout,
		CommitInterval: commitInterval,
	}

	return &kafkaConfig, nil
}
