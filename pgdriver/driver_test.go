package pgdriver

import (
	"database/sql"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

func init() {
	cfg := postgreDriverConfig{
		User:     "noxiouz",
		Database: "distribution",
	}

	db, err := sql.Open(driverSQLName, cfg.ConnectionString())
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
		ID	SERIAL 	PRIMARY KEY,
		NAME VARCHAR(256) NOT NULL UNIQUE
		);`); err != nil {
		panic(err)
	}

	if _, err := db.Exec(`CREATE TABLE mfs (
				PATH 	VARCHAR(256) PRIMARY KEY UNIQUE,
				PARENT	VARCHAR(256) NOT NULL,
				DIR		BOOLEAN NOT NULL,
				SIZE 	INTEGER NOT NULL,
				MODTIME TIME NOT NULL,
				MDSID INT references mds(ID)
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
