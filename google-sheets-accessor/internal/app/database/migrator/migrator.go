package migrator

//nolint:revive // drivers
import (
	"database/sql"
	"errors"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/database"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

func MigrateUp(config database.Config) (bool, error) {
	db, err := sql.Open("postgres", config.ToString())
	if err != nil {
		return false, err
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: "publicbox_migrations",
	})
	if err != nil {
		return false, err
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://internal/app/database/migrations/",
		"postgres",
		driver,
	)
	if err != nil {
		return false, err
	}
	err = m.Up()
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
