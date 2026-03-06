package app

import (
	"os"
	"strings"
)

type Config struct {
	Port                              string
	APIVersion                        string
	TopicInterestingMail              string
	TopicHireEvent                    string
	TopicApplicationsSyncUnprocessed  string
	TopicApplicationsSyncProcessed    string
	TopicApplicationUpdateUnprocessed string
	TopicApplicationUpdateProcessed   string
}

func LoadConfig() *Config {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8085"
	}

	apiVersion := strings.TrimSpace(os.Getenv("API_VERSION"))
	if apiVersion == "" {
		apiVersion = "1"
	}

	config := Config{
		Port:                              port,
		APIVersion:                        apiVersion,
		TopicInterestingMail:              strings.TrimSpace(os.Getenv("KAFKA_TOPIC_INTERESTING_MAIL")),
		TopicHireEvent:                    strings.TrimSpace(os.Getenv("KAFKA_TOPIC_HIRE_EVENT")),
		TopicApplicationsSyncUnprocessed:  strings.TrimSpace(os.Getenv("KAFKA_TOPIC_APPLICATIONS_SYNC_UNPROCESSED")),
		TopicApplicationsSyncProcessed:    strings.TrimSpace(os.Getenv("KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED")),
		TopicApplicationUpdateUnprocessed: strings.TrimSpace(os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED")),
		TopicApplicationUpdateProcessed:   strings.TrimSpace(os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED")),
	}

	return &config
}
