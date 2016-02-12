package pgcluster

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type Role int

const (
	MASTER Role = iota
	SLAVE
)

var (
	// ErrDublicatedDataSource means that connStrings contains duplicated items
	ErrDublicatedDataSource = errors.New("duplicated data source")
	ErrZeroDataSource       = errors.New("data source muts contain at least one item")
)

// Cluster represents a PostgreSQL cluster keeping track of a current master
type Cluster struct {
	dbs map[string]*sql.DB

	currentMaster atomic.Value

	stopCh chan struct{}
}

// NewPostgreSQLCluster creates Cluster. Drivername can be specified,
// but must point to a PostgreSQL driver.
func NewPostgreSQLCluster(drivername string, connStrings []string) (*Cluster, error) {
	cleanUpDBs := func(dbs map[string]*sql.DB) {
		for _, db := range dbs {
			db.Close()
		}
	}

	dbs := make(map[string]*sql.DB, len(connStrings))

	if len(connStrings) == 0 {
		return nil, ErrZeroDataSource
	}

	for _, connStr := range connStrings {
		db, err := sql.Open(drivername, connStr)
		if err != nil {
			cleanUpDBs(dbs)
			return nil, err
		}

		if _, ok := dbs[connStr]; ok {
			cleanUpDBs(dbs)
			return nil, ErrDublicatedDataSource
		}

		dbs[connStr] = db
	}

	cluster := &Cluster{
		dbs: dbs,

		stopCh: make(chan struct{}),
	}

	// electMaster relies on the fact that the value is Stored
	cluster.currentMaster.Store(dbs[connStrings[0]])

	cluster.electMaster()

	return cluster, nil
}

// Close closes connections per each db contained in Cluster.
// An error fron each Close is collected.
func (c *Cluster) Close() error {
	close(c.stopCh)

	var errors []error
	for _, db := range c.dbs {
		if err := db.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) != 0 {
		return fmt.Errorf("%v", errors)
	}

	return nil
}

// DB returns *sql.DB suggested to be a master in the cluster.
// Current implementation checks master every 5 seconds.
// However the proper approach is to reelect a master after disconnection error.
func (c *Cluster) DB(role Role) *sql.DB {
	switch role {
	case MASTER:
		// It is always set. Even if there's no master at all.
		return c.currentMaster.Load().(*sql.DB)
	case SLAVE:
		// TODO: NOT IMPLEMENTED
		// It is always set. Even if there's no master at all.
		return c.currentMaster.Load().(*sql.DB)
	default:
		panic("invalid Role requested")
	}
}

func (c *Cluster) overwatch() {
	for {
		select {
		case <-time.After(time.Second * 5):
			c.electMaster()

		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) electMaster() {
	currentDB := c.currentMaster.Load().(*sql.DB)
	if isMaster(currentDB) {
		return
	}

	for _, db := range c.dbs {
		// TODO: skip currentDB
		if isMaster(db) {
			c.currentMaster.Store(db)
		}
	}
}

func isMaster(db *sql.DB) bool {
	var isInRecovery bool
	if err := db.QueryRow("SELECT pg_is_in_recovery()").Scan(&isInRecovery); err != nil {
		return false
	}

	// NOTE: it is master, if it is not in recovery
	return !isInRecovery
}
