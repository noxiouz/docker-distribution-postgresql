package pgdriver

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"

	"github.com/mitchellh/mapstructure"

	_ "github.com/lib/pq"
)

var (
	_              = log.Print
	notimplemented = errors.New("not implemneted yet")
	// ErrNoDirectURLForDirectory means that URLFor points to a directory
	ErrNoDirectURLForDirectory = errors.New("no direct URL for directory")
)

const (
	driverSQLName = "postgres"
	driverName    = "postgres"

	tableMeta = "mfs"
	tableMDS  = "mds"
)

func init() {
	factory.Register(driverName, &factoryPostgreDriver{})
}

type postgreDriverConfig struct {
	User     string
	Database string
}

func (p *postgreDriverConfig) ConnectionString() string {
	return fmt.Sprintf("user=%s dbname=%s sslmode=disable", p.User, p.Database)
}

type factoryPostgreDriver struct{}

func (f *factoryPostgreDriver) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	var (
		config postgreDriverConfig
	)

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &config,
	})
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(parameters)
	if err != nil {
		return nil, err
	}

	return pgdriverNew(&config)
}

type baseEmbeded struct {
	base.Base
}

// Driver stores metadata in MongoDB and data in a remote storage with HTTP API
type Driver struct {
	baseEmbeded

	db *sql.DB
}

func pgdriverNew(cfg *postgreDriverConfig) (*Driver, error) {
	db, err := sql.Open(driverSQLName, cfg.ConnectionString())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// NOTE: move it to a separate SQL file
	// NOTE: create index over Parent
	d := &Driver{
		db: db,
	}
	return d, nil
}

// Name returns the driver name
func (d *Driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *Driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	return nil, notimplemented
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *Driver) PutContent(ctx context.Context, path string, content []byte) error {
	fmt.Println(path)
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// checkStmt check if the file or dir exists and returns its type
	checkStmt, err := tx.Prepare("SELECT dir FROM mfs WHERE path=$1 LIMIT 1")
	if err != nil {
		return err
	}

	// insertStmt inserts metainformation about file or dir
	insertStmt, err := tx.Prepare("INSERT INTO mfs (path, parent, dir, size, modtime) VALUES ($1, $2, $3, $4, now())")
	if err != nil {
		return err
	}

	// Check and insert file
	var isDir = false
	switch err := checkStmt.QueryRow(path).Scan(&isDir); err {
	case nil:
		if isDir {
			return fmt.Errorf("unable to rewrite directory by file: %s", path)
		}
		if _, err := tx.Exec("DELETE FROM mfs WHERE path=$1", path); err != nil {
			return err
		}
	case sql.ErrNoRows:
		// pass
	default:
		return err
	}
	// NOTE: may be update would be usefull
	if _, err := insertStmt.Exec(path, filepath.Dir(path), false, int64(len(content))); err != nil {
		return err
	}

	parent := filepath.Dir(path)
DIRECTORY_CREATION_LOOP:
	for dir, filename := filepath.Dir(parent), filepath.Base(parent); filename != "/" && filename != "."; dir, filename = filepath.Dir(dir), filepath.Base(dir) {
		var (
			fullpath = filepath.Join(dir, filename)
			isDir    = false
		)

		switch err := checkStmt.QueryRow(fullpath).Scan(&isDir); err {
		case nil:
			if !isDir {
				return fmt.Errorf("unable to rewrite file by directory: %s", path)
			}
			break DIRECTORY_CREATION_LOOP
		case sql.ErrNoRows:
			// pass
		default:
			return err
		}

		_, err = insertStmt.Exec(fullpath, dir, true, 0)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *Driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return nil, notimplemented
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *Driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {
	return 0, notimplemented
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *Driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	return nil, notimplemented
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *Driver) List(ctx context.Context, path string) ([]string, error) {
	//NOTE: should I use Tx?
	if path != "/" {
		var ph interface{}
		switch err := d.db.QueryRow("SELECT 1 FROM mfs WHERE path=$1", path).Scan(&ph); err {
		case sql.ErrNoRows:
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		case nil:
			// pass
		default:
			return nil, err
		}
	}

	rows, err := d.db.Query("SELECT path FROM mfs WHERE parent=$1", path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var listing []string
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		listing = append(listing, item)
	}
	return listing, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *Driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	return notimplemented
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *Driver) Delete(ctx context.Context, path string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var deleted []string
	if path != "/" {
		deleted = append(deleted, path)
		if _, err := tx.Exec("DELETE FROM mfs WHERE mfs.path = $1", path); err != nil {
			return err
		}
	}

	// TODO: we don't need path actually. Our target is MDS.ID to mark deleted files
	rows, err := tx.Query(`
	WITH RECURSIVE t(path) AS (
	        SELECT path FROM mfs WHERE parent = $1
	    UNION ALL
	        SELECT mfs.path FROM t, mfs WHERE mfs.parent = t.path
	)
	DELETE FROM mfs USING t WHERE mfs.path = t.path RETURNING mfs.path;
	`, path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		deleted = append(deleted, name)
	}

	if err := tx.Commit(); err != nil {
		return nil
	}
	// TODO: mark fields in MDS table

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
func (d *Driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", notimplemented
}
