package pgdriver

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"

	"github.com/mitchellh/mapstructure"
	"github.com/noxiouz/go-postgresql-cluster/pgcluster"

	// PostgreSQL backend for database/sql
	_ "github.com/lib/pq"
)

var (
	// ErrNoDirectURLForDirectory means that URLFor points to a directory
	ErrNoDirectURLForDirectory = errors.New("no direct URL for directory")
)

const (
	driverSQLName = "postgres"
	driverName    = "postgres"

	tableMeta = "mfs"
	tableMDS  = "mds"

	// UserNameKey is used to get the user name from a user context
	// NOTE: This const is defined since > 2.3.0:
	// github.com/docker/distribution/registry/auth
	// use it after upgrade
	UserNameKey = "auth.user.name"
)

func init() {
	factory.Register(driverName, &factoryPostgreDriver{})
}

type postgreDriverConfig struct {
	URLs           []string
	ConnectTimeout time.Duration
	MaxOpenConns   int
	// pointer is here to distinguish 0 vlaue from zerovalue by comparing with `nil`
	MaxIdleConns *int

	Type    string
	Options map[string]interface{}
}

type factoryPostgreDriver struct{}

func decodeConfig(parameters map[string]interface{}, config interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           config,
	})

	if err != nil {
		return err
	}

	return decoder.Decode(parameters)
}

func (f *factoryPostgreDriver) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	var (
		config postgreDriverConfig
	)

	if err := decodeConfig(parameters, &config); err != nil {
		return nil, err
	}

	return pgdriverNew(&config)
}

type driver struct {
	cluster *pgcluster.Cluster
	storage BinaryStorage
}

type baseEmbed struct {
	base.Base
}

// Driver implements Storage interface. It uses PostgreSQL and plain KV storage to save data
type Driver struct {
	baseEmbed
}

func pgdriverNew(cfg *postgreDriverConfig) (*Driver, error) {
	var (
		st  BinaryStorage
		err error
	)
	switch cfg.Type {
	case "inmemory":
		st, err = newInMemory()
	case "mds":
		st, err = newMDSBinStorage(cfg.Options)
	default:
		return nil, fmt.Errorf("Unsupported binadry storage backend %s", cfg.Type)
	}
	if err != nil {
		return nil, err
	}

	cluster, err := pgcluster.NewPostgreSQLCluster(driverSQLName, cfg.URLs)
	if err != nil {
		return nil, err
	}

	if err := cluster.DB(pgcluster.MASTER).Ping(); err != nil {
		return nil, err
	}

	if cfg.MaxOpenConns != 0 {
		cluster.SetMaxOpenConns(cfg.MaxOpenConns)
	}

	if cfg.MaxIdleConns != nil {
		cluster.SetMaxIdleConns(*cfg.MaxIdleConns)
	}

	d := &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					cluster: cluster,
					storage: st,
				},
			},
		},
	}
	return d, nil
}

// Name returns the driver name
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	key, err := d.getKey(ctx, path)
	if err != nil {
		return nil, err
	}

	reader, err := d.storage.Get(key, 0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var output = new(bytes.Buffer)
	if _, err := io.Copy(output, reader); err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func (d *driver) getKey(ctx context.Context, path string) ([]byte, error) {
	return getKeyViaDB(ctx, d.cluster.DB(pgcluster.MASTER), path)
}

type rowQuerier interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

func getKeyViaDB(ctx context.Context, db rowQuerier, path string) ([]byte, error) {
	var keymeta []byte
	err := db.QueryRow("SELECT mds.keymeta FROM mfs JOIN mds ON (mfs.mdsid = mds.id) WHERE mfs.path = $1", path).Scan(&keymeta)
	switch err {
	case sql.ErrNoRows:
		// NOTE: actually it also means that the path is a directory
		return nil, storagedriver.PathNotFoundError{Path: path}
	case nil:
		return keymeta, nil
	default:
		return nil, err
	}
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	if _, err := d.WriteStream(ctx, path, 0, bytes.NewReader(content)); err != nil {
		return err
	}
	return nil
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {
	// NOTE: right now writting with offset is not supported by MDS, so there's no point to implemnet it now.
	// It could be added to testing backend, but it should UPDATE if key already exist and insert if it does not.
	if offset != 0 {
		tx, err := d.cluster.DB(pgcluster.MASTER).Begin()
		if err != nil {
			return 0, err
		}
		defer tx.Rollback()

		keymeta, err := getKeyViaDB(ctx, tx, path)
		if err != nil {
			return 0, err
		}

		nn, err = d.storage.Append(keymeta, reader, offset)
		switch err {
		case nil:
			// NOTE: update size
			_, err := tx.Exec("UPDATE mfs SET size = $1 WHERE (path = $2)", nn+offset, path)
			if err != nil {
				return nn, err
			}

			return nn, tx.Commit()
		case ErrAppendUnsupported:
			return nn, &storagedriver.Error{
				DriverName: driverName,
				Enclosed:   fmt.Errorf("BinaryStorage does not support writing with non-zero offset"),
			}
		default:
			return nn, err
		}
	}

	var owner = ctx.Value(UserNameKey)

	key, nn, err := d.storage.Store(reader)
	if err != nil {
		return nn, err
	}
	// TODO: delete the key if tx is rollbacked

	tx, err := d.cluster.DB(pgcluster.MASTER).Begin()
	if err != nil {
		return nn, err
	}
	defer tx.Rollback()

	// checkStmt check if the file or dir exists and returns its type
	checkStmt, err := tx.Prepare("SELECT dir FROM mfs WHERE path=$1")
	if err != nil {
		return 0, err
	}

	// insertStmt inserts metainformation about file or dir
	insertStmt, err := tx.Prepare("INSERT INTO mfs (path, parent, dir, size, modtime, mdsid, owner) VALUES ($1, $2, $3, $4, now(), $5, $6)")
	if err != nil {
		return
	}

	var mdsid int64
	if err := d.cluster.DB(pgcluster.MASTER).QueryRow("INSERT INTO mds (keymeta) VALUES ($1) RETURNING ID", key).Scan(&mdsid); err != nil {
		return 0, err
	}

	// Check and insert file
	var isDir = false
	switch err := checkStmt.QueryRow(path).Scan(&isDir); err {
	case nil:
		if isDir {
			return 0, fmt.Errorf("unable to rewrite directory by file: %s", path)
		}
		if _, err := tx.Exec("DELETE FROM mfs WHERE path=$1", path); err != nil {
			return 0, err
		}
	case sql.ErrNoRows:
		// pass
	default:
		return 0, err
	}

	// NOTE: may be update would be useful
	// NOTE: calculate size properly
	if _, err := insertStmt.Exec(path, filepath.Dir(path), false, offset+nn, mdsid, owner); err != nil {
		return 0, err
	}

	// TODO: wrap into a function
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
				return nn, fmt.Errorf("unable to rewrite file by directory: %s", path)
			}
			break DIRECTORY_CREATION_LOOP
		case sql.ErrNoRows:
			// pass
		default:
			return nn, err
		}

		_, err = insertStmt.Exec(fullpath, dir, true, 0, nil, owner)
		if err != nil {
			return nn, err
		}
	}

	return nn, tx.Commit()
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	key, err := d.getKey(ctx, path)
	if err != nil {
		return nil, err
	}
	return d.storage.Get(key, offset)
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	info := storagedriver.FileInfoFields{
		Path: path,
	}

	// NOTE: should size of directory be evaluated as total size of its childs?
	err := d.cluster.DB(pgcluster.MASTER).QueryRow("SELECT dir, size, modtime FROM mfs WHERE path=$1", path).Scan(&info.IsDir, &info.Size, &info.ModTime)
	switch err {
	case sql.ErrNoRows:
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	case nil:
		return &storagedriver.FileInfoInternal{info}, nil
	default:
		return nil, err
	}
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	//NOTE: should I use Tx?
	if path != "/" {
		var ph interface{}
		switch err := d.cluster.DB(pgcluster.MASTER).QueryRow("SELECT 1 FROM mfs WHERE path=$1", path).Scan(&ph); err {
		case sql.ErrNoRows:
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		case nil:
			// pass
		default:
			return nil, err
		}
	}

	rows, err := d.cluster.DB(pgcluster.MASTER).Query("SELECT path FROM mfs WHERE parent=$1", path)
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
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	tx, err := d.cluster.DB(pgcluster.MASTER).Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	checkStmt, err := tx.Prepare("SELECT dir FROM mfs WHERE path=$1")
	if err != nil {
		return err
	}
	defer checkStmt.Close()

	// Check that the source exists and is a file.
	var isDir = false
	switch err := checkStmt.QueryRow(sourcePath).Scan(&isDir); err {
	case sql.ErrNoRows:
		return storagedriver.PathNotFoundError{Path: sourcePath}
	case nil:
		if isDir {
			return fmt.Errorf("source `%s` is a directory. Moving directories is not supported", sourcePath)
		}
	default:
		return err
	}

	var owner = ctx.Value(UserNameKey)

	// Check that the dest is not a directory.
	switch err := checkStmt.QueryRow(destPath).Scan(&isDir); err {
	case sql.ErrNoRows:
		parent := filepath.Dir(destPath)
		var (
			size  int64
			mdsid sql.NullInt64
		)

		if err := tx.QueryRow(`DELETE FROM mfs WHERE path = $1 RETURNING size, mdsid`, sourcePath).Scan(&size, &mdsid); err != nil {
			return err
		}

		_, err = tx.Exec(`INSERT INTO mfs (path, parent, dir, size, modtime, mdsid, owner) VALUES ($1, $2, false, $3, now(), $4, $5)`, destPath, parent, size, mdsid, owner)
		if err != nil {
			return err
		}

		// checkStmt check if the file or dir exists and returns its type
		checkStmt, err := tx.Prepare("SELECT dir FROM mfs WHERE path=$1")
		if err != nil {
			return err
		}

		// insertStmt inserts metainformation about file or dir
		insertStmt, err := tx.Prepare("INSERT INTO mfs (path, parent, dir, size, modtime, mdsid, owner) VALUES ($1, $2, $3, $4, now(), $5, $6)")
		if err != nil {
			return err
		}
	DIRECTORY_CREATION_LOOP:
		for dir, filename := filepath.Dir(parent), filepath.Base(parent); filename != "/" && filename != "."; dir, filename = filepath.Dir(dir), filepath.Base(dir) {
			var (
				fullpath = filepath.Join(dir, filename)
				isDir    = false
			)

			switch err := checkStmt.QueryRow(fullpath).Scan(&isDir); err {
			case nil:
				if !isDir {
					return fmt.Errorf("unable to rewrite file by directory: %s", destPath)
				}
				break DIRECTORY_CREATION_LOOP
			case sql.ErrNoRows:
				// pass
			default:
				return err
			}

			_, err = insertStmt.Exec(fullpath, dir, true, 0, nil, owner)
			if err != nil {
				return err
			}
		}

	case nil:
		if isDir {
			return fmt.Errorf("destination `%s` is a directory. Moving directories is not supported", destPath)
		}
		// TODO: looks ugly. Actually I can merge previous queries here by adding dir = true
		// Delete source record and update dest record with some fields
		_, err := tx.Exec(`
			WITH t AS (DELETE FROM mfs WHERE path = $1 RETURNING size, mdsid)
			UPDATE mfs SET (size, modtime, mdsid) = (t.size, now(), t.mdsid)
			FROM t WHERE mfs.path = $2;`, sourcePath, destPath)
		if err != nil {
			return err
		}
	default:
		return err
	}

	return tx.Commit()
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	tx, err := d.cluster.DB(pgcluster.MASTER).Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var (
		// NOTE: intended to be used to mark files in MDS table
		deleted []sql.NullInt64

		mdsid sql.NullInt64
		isDir = false
	)

	if path != "/" {
		err = tx.QueryRow("DELETE FROM mfs WHERE mfs.path = $1 RETURNING mfs.mdsid, mfs.dir", path).Scan(&mdsid, &isDir)
		switch err {
		case nil:
			if mdsid.Valid {
				deleted = append(deleted, mdsid)
			}
		case sql.ErrNoRows:
			return storagedriver.PathNotFoundError{Path: path}
		default:
			return err
		}

	}

	// NOTE: scan for childs only if a directory is being deleted
	if isDir {
		// TODO: it's possible to add optimization for dir only RECURSIVE scanning
		rows, err := tx.Query(`
			WITH RECURSIVE t(path) AS (
			        SELECT path FROM mfs WHERE parent = $1
			    UNION ALL
			        SELECT mfs.path FROM t, mfs WHERE mfs.parent = t.path
			)
			DELETE FROM mfs USING t WHERE mfs.path = t.path RETURNING mfs.mdsid;
		`, path)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&mdsid); err != nil {
				return err
			}

			if mdsid.Valid {
				deleted = append(deleted, mdsid)
			}
		}
	}

	// TODO: mark fields in MDS table before commit from `deleted` array
	return tx.Commit()
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	// TODO: implement it
	return "", storagedriver.ErrUnsupportedMethod{}
}
