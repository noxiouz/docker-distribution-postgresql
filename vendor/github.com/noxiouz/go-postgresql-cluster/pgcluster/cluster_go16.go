// +build go1.6

package pgcluster

import (
	"time"
)

// SetConnMaxLifetime sets the maximum amount of time
// a connection may be reused for each memeber of a cluster
func (c *Cluster) SetConnMaxLifetime(d time.Duration) {
	for _, db := range c.dbs {
		db.SetConnMaxLifetime(d)
	}
}
