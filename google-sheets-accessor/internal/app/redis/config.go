package redis

import (
	"os"
	"strconv"
)

const (
	defaultHost = "redis-google-sheets-accessor"
	defaultPort = 6379
	defaultDB   = 0
)

type Config struct {
	Host     string
	Port     int
	DB       int
	Password string
}

func LoadConfig() *Config {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = defaultHost
	}

	port := defaultPort

	if portRaw := os.Getenv("REDIS_PORT"); portRaw != "" {
		if parsedPort, err := strconv.Atoi(portRaw); err == nil {
			port = parsedPort
		}
	}

	db := defaultDB

	if dbRaw := os.Getenv("REDIS_DB"); dbRaw != "" {
		if parsedDB, err := strconv.Atoi(dbRaw); err == nil {
			db = parsedDB
		}
	}

	password := os.Getenv("REDIS_PASSWORD")

	return &Config{
		Host:     host,
		Port:     port,
		DB:       db,
		Password: password,
	}
}
