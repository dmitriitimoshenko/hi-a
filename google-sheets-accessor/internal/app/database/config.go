package database

import (
	"fmt"
	"os"
)

type config struct {
	Host     string
	Port     string
	Password string
	User     string
	Name     string
	SSLMode  string
}

type databaseConfig struct {
	config config
}

type Config interface {
	ToString() string
}

func LoadConfig() Config {
	return &databaseConfig{
		config: config{
			Host:     os.Getenv("GSA_DB_HOST"),
			Port:     os.Getenv("GSA_DB_PORT"),
			Password: os.Getenv("GSA_DB_PASS"),
			User:     os.Getenv("GSA_DB_USER"),
			Name:     os.Getenv("GSA_DB_NAME"),
			SSLMode:  "disable",
		},
	}
}

func (c *databaseConfig) ToString() string {
	str := fmt.Sprintf(
		"user=%s password=%s database=%s host=%s sslmode=%s",
		c.config.User, c.config.Password, c.config.Name, c.config.Host, c.config.SSLMode,
	)

	return str
}
