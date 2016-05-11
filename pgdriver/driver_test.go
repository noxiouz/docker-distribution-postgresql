package pgdriver

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func init() {

	fromEnvOrDefault := func(envvar, defval string) string {
		val := os.Getenv(envvar)
		if val != "" {
			return val
		}
		return defval
	}

	URLs := fromEnvOrDefault("PG_URLS", "postgres://noxiouz@localhost:5432/distribution?sslmode=disable")

	var idleConns = 5
	cfg := postgreDriverConfig{
		MaxOpenConns: 10,
		MaxIdleConns: &idleConns,
		URLs:         strings.Split(URLs, " "),
		Type:         "inmemory",
	}

	db, err := sql.Open(driverSQLName, cfg.URLs[0])
	if err != nil {
		panic(err)
	}
	defer db.Close()

	clean := func() error {
		if _, err := db.Exec(`DROP TABLE IF EXISTS mfs`); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS mds`); err != nil {
			return err
		}
		return nil
	}

	if err := clean(); err != nil {
		panic(err)
	}

	// create tables
	if _, err := db.Exec(`CREATE TABLE mds (
			KEY 	TEXT PRIMARY KEY,
			MDSFILEINFO TEXT NOT NULL,
			DELETED BOOLEAN NOT NULL DEFAULT FALSE
	    );`); err != nil {
		panic(err)
	}

	if _, err := db.Exec(`CREATE TABLE mfs (
				PATH 	TEXT PRIMARY KEY UNIQUE,
				PARENT	TEXT NOT NULL,
				DIR		BOOLEAN NOT NULL,
				SIZE 	INTEGER NOT NULL,
				MODTIME TIME NOT NULL,
				KEY   TEXT,
				OWNER   TEXT
			);`); err != nil {
		panic(err)
	}
	if _, err := db.Exec(`CREATE INDEX parent_idx ON mfs (parent);`); err != nil {
		panic(err)
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return pgdriverNew(&cfg)
	}, testsuites.NeverSkip)
}
