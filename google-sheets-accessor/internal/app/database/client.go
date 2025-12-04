package database

import (
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewConnection(config string) (*gorm.DB, error) {
	logCfg := logger.Config{
		SlowThreshold: time.Millisecond * 500,
	}
	gormLogger := logger.New(log.New(os.Stdout, "", log.LstdFlags), logCfg)

	db, err := gorm.Open(
		postgres.Open(config),
		&gorm.Config{
			Logger: gormLogger,
		},
	)
	if err != nil {
		return db, err
	}

	return db, nil
}
