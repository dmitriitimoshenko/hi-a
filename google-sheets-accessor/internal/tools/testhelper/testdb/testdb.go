package testdb

import (
	"fmt"
	"sync/atomic"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var dbSeq atomic.Uint64

func newDSN() string {
	id := dbSeq.Add(1)

	return fmt.Sprintf("file:testdb_%d?mode=memory&cache=shared&_fk=1", id)
}

func NewSQLiteTestDB(t *testing.T, models ...any) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(newDSN()), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	if err := db.Exec("PRAGMA foreign_keys = ON").Error; err != nil {
		t.Fatalf("failed to enable foreign keys: %v", err)
	}

	if len(models) > 0 {
		if err := db.AutoMigrate(models...); err != nil {
			t.Fatalf("failed to migrate: %v", err)
		}
	}

	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() {
			_ = sqlDB.Close()
		})
	}

	return db
}
