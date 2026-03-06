package kafka

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	Brokers        []string
	GroupID        string
	ClientID       string
	DialTimeout    time.Duration
	CommitInterval time.Duration
}

func LoadConfig() (*Config, error) {
	kafkaServer := strings.TrimSpace(os.Getenv("KAFKA_SERVER"))
	if kafkaServer == "" {
		return nil, errors.New("KAFKA_SERVER is required")
	}

	var brokers []string

	for _, broker := range strings.Split(kafkaServer, ",") {
		trimmedBroker := strings.TrimSpace(broker)
		if trimmedBroker == "" {
			continue
		}

		brokers = append(brokers, trimmedBroker)
	}

	if len(brokers) == 0 {
		return nil, errors.New("KAFKA_SERVER must include at least one broker")
	}

	config := Config{
		Brokers:        brokers,
		GroupID:        strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP")),
		ClientID:       strings.TrimSpace(os.Getenv("KAFKA_CLIENT_ID")),
		DialTimeout:    10 * time.Second,
		CommitInterval: 2 * time.Second,
	}

	return &config, nil
}
