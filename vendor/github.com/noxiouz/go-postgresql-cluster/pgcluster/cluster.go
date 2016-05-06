package pgcluster

import (
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"sync/atomic"
	"time"
)

// Role is a role in the cluster of requested node
type Role int

const (
	// MASTER is the master one in a cluster
	MASTER Role = iota
	// SLAVE is a replication node in a cluster
	SLAVE
)

var (
	// ErrDublicatedDataSource means that connStrings contains duplicated items
	ErrDublicatedDataSource = errors.New("duplicated data source")
	// ErrZeroDataSource means that an empty connStrings was passed
	ErrZeroDataSource = errors.New("data source must contain at least one item")

	pgClusterStats = expvar.NewMap("pgcluster_stats")
	masterVar      = new(expvar.Int)
	lastElection   = new(expvar.String)
)

func init() {
	pgClusterStats.Set("master", masterVar)
	pgClusterStats.Set("last_election", lastElection)
}

// Cluster represents a PostgreSQL cluster keeping track of a current master
type Cluster struct {
	dbs []*sql.DB

	currentMaster atomic.Value

	stopCh chan struct{}
}

// NewPostgreSQLCluster creates Cluster. Drivername can be specified,
// but must point to a PostgreSQL driver.
func NewPostgreSQLCluster(drivername string, connStrings []string) (*Cluster, error) {
	cleanUpDBs := func(dbs []*sql.DB) {
		for _, db := range dbs {
			db.Close()
		}
	}

	dedup := make(map[string]struct{})
	dbs := make([]*sql.DB, 0, len(connStrings))

	if len(connStrings) == 0 {
		return nil, ErrZeroDataSource
	}

	for _, connStr := range connStrings {
		if _, ok := dedup[connStr]; ok {
			cleanUpDBs(dbs)
			return nil, ErrDublicatedDataSource
		}
		dedup[connStr] = struct{}{}

		db, err := sql.Open(drivername, connStr)
		if err != nil {
			cleanUpDBs(dbs)
			return nil, err
		}

		dbs = append(dbs, db)
	}

	cluster := &Cluster{
		dbs: dbs,

		stopCh: make(chan struct{}),
	}

	// electMaster relies on the fact that the value is Stored,
	// so pick the random one
	cluster.setMaster(0, dbs[0])

	cluster.electMaster()

	go cluster.overwatch()

	return cluster, nil
}

// NOTE: SetConnMaxLifetime implement fot go1.6 only

// SetMaxIdleConns sets the maximum number of connections
// in the idle connection pool for each memeber of a cluster
func (c *Cluster) SetMaxIdleConns(n int) {
	for _, db := range c.dbs {
		db.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to the database for each memeber of a cluster
func (c *Cluster) SetMaxOpenConns(n int) {
	for _, db := range c.dbs {
		db.SetMaxOpenConns(n)
	}
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

func (c *Cluster) setMaster(pos int, db *sql.DB) {
	masterVar.Set(int64(pos))
	c.currentMaster.Store(db)
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

// ReElect verifies if the current master is really master.
// New master will be elected if needed. This can be called after connections
// errors detection.
func (c *Cluster) ReElect() {
	c.electMaster()
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
	lastElection.Set(time.Now().String())
	currentDB := c.currentMaster.Load().(*sql.DB)
	if isMaster(currentDB) {
		return
	}

	for pos, db := range c.dbs {
		// TODO: skip currentDB
		if isMaster(db) {
			c.setMaster(pos, db)
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
