package mockdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type noopConnector struct{}

func (noopConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &noopConn{}, nil
}

func (noopConnector) Driver() driver.Driver {
	return noopDriver{}
}

type noopDriver struct{}

func (noopDriver) Open(string) (driver.Conn, error) {
	return &noopConn{}, nil
}

type noopConn struct{}

func (c *noopConn) Prepare(string) (driver.Stmt, error) {
	return noopStmt{}, nil
}

func (c *noopConn) Close() error { return nil }

func (c *noopConn) Begin() (driver.Tx, error) {
	return &noopTx{}, nil
}

func (c *noopConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &noopTx{}, nil
}

func (c *noopConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return noopResult{}, nil
}

func (c *noopConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &noopRows{}, nil
}

type noopStmt struct{}

func (noopStmt) Close() error { return nil }

func (noopStmt) NumInput() int {
	return -1
}

func (noopStmt) Exec([]driver.Value) (driver.Result, error) {
	return noopResult{}, nil
}

func (noopStmt) Query([]driver.Value) (driver.Rows, error) {
	return &noopRows{}, nil
}

type noopTx struct{}

func (noopTx) Commit() error   { return nil }
func (noopTx) Rollback() error { return nil }

type noopResult struct{}

func (noopResult) LastInsertId() (int64, error) { return 0, nil }
func (noopResult) RowsAffected() (int64, error) { return 0, nil }

type noopRows struct{}

func (noopRows) Columns() []string { return []string{} }
func (noopRows) Close() error      { return nil }
func (noopRows) Next([]driver.Value) error {
	return io.EOF
}

func NewTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	sqlDB := sql.OpenDB(noopConnector{})
	db, err := gorm.Open(
		postgres.New(postgres.Config{
			Conn: sqlDB,
		}),
		&gorm.Config{
			DisableAutomaticPing:   true,
			SkipDefaultTransaction: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to init test db: %v", err)
	}

	return db.Session(&gorm.Session{DryRun: true})
}

func NewUnmigratedDB(t *testing.T) *gorm.DB {
	t.Helper()

	db := NewTestDB(t)
	db.Callback().Create().Before("gorm:create").Register("force_error", func(tx *gorm.DB) {
		tx.AddError(errors.New("forced error"))
	})
	db.Callback().Update().Before("gorm:update").Register("force_error", func(tx *gorm.DB) {
		tx.AddError(errors.New("forced error"))
	})

	return db
}
