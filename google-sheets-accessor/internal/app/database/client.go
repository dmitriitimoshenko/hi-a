package database

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewConnection(config string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(config), &gorm.Config{})
	if err != nil {
		return db, err
	}
	return db, nil
}
