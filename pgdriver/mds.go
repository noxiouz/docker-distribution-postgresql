package pgdriver

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"

	"github.com/docker/distribution/context"

	"github.com/noxiouz/go-postgresql-cluster/pgcluster"
	"github.com/noxiouz/mds"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const (
	tableMDS = "mds"
)

type metaInfo struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ID   string `json:"id"`
}

func (m *metaInfo) Value() (sqldriver.Value, error) {
	return json.Marshal(m)
}

func (m *metaInfo) Scan(src interface{}) error {
	switch body := src.(type) {
	case []byte:
		return json.Unmarshal(body, m)
	default:
		return fmt.Errorf("can not Scan from non []byte type: %v", reflect.TypeOf(body))
	}
}

type mdsBinStorage struct {
	*pgcluster.Cluster
	Storage   *mds.Client
	Namespace string
}

func newMDSBinStorage(cluster *pgcluster.Cluster, parameters map[string]interface{}) (KVStorage, error) {
	var config struct {
		mds.Config `mapstructure:",squash"`
		Namespace  string
	}

	if err := decodeConfig(parameters, &config); err != nil {
		return nil, err
	}
	mdsClient, err := mds.NewClient(config.Config)
	if err != nil {
		return nil, err
	}

	return &mdsBinStorage{
		Cluster:   cluster,
		Storage:   mdsClient,
		Namespace: config.Namespace,
	}, nil
}

func (m *mdsBinStorage) Store(ctx context.Context, key string, data io.Reader) (int64, error) {
	uinfo, err := m.Storage.Upload(m.Namespace, key, ioutil.NopCloser(data))
	if err != nil {
		return 0, err
	}

	var meta = &metaInfo{
		Key:  uinfo.Key,
		Size: int64(uinfo.Size),
		ID:   uinfo.ID,
	}

	_, err = m.DB(pgcluster.MASTER).Exec("INSERT INTO mds (key, mdsfileinfo) VALUES ($1, $2)", key, meta)
	if err != nil {
		if mdserr := m.Storage.Delete(m.Namespace, uinfo.Key); mdserr != nil {
			context.GetLoggerWithFields(ctx, map[interface{}]interface{}{"error": mdserr, "key": uinfo.Key}).Error("can not clean MDS after DB error")
		}
		return 0, err
	}

	return meta.Size, nil
}

func (m *mdsBinStorage) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	mdskey, err := m.getMDSKey(ctx, key)
	if err != nil {
		return nil, err
	}

	return m.Storage.Get(m.Namespace, mdskey, uint64(offset))
}

func (m *mdsBinStorage) Delete(ctx context.Context, key string) error {
	mdskey, err := m.getMDSKey(ctx, key)
	if err != nil {
		return err
	}

	if err = m.Storage.Delete(m.Namespace, mdskey); err != nil {
		return err
	}

	// Mark deleted
	_, err = m.DB(pgcluster.MASTER).Exec("UPDATE mds SET deleted = true WHERE (key = $1)", key)
	if err != nil {
		context.GetLogger(ctx).Errorf("update metainfo about deleted key %s error: %v", key, err)
	}

	return nil
}

func (m *mdsBinStorage) Append(ctx context.Context, key string, data io.Reader) (int64, error) {
	_, err := m.getMDSKey(ctx, key)
	switch err.(type) {
	case storagedriver.PathNotFoundError:
		return m.Store(ctx, key, data)
	case nil:
		return 0, ErrAppendUnsupported
	default:
		return 0, err
	}
}

func (m *mdsBinStorage) URLFor(ctx context.Context, key string) (string, error) {
	mdskey, err := m.getMDSKey(ctx, key)
	if err != nil {
		return "", err
	}

	return m.Storage.ReadURL(m.Namespace, mdskey), nil
}

func (m *mdsBinStorage) getMDSKey(ctx context.Context, key string) (string, error) {
	var mdsmeta metaInfo
	err := m.DB(pgcluster.MASTER).QueryRow("SELECT mdsfileinfo FROM mds WHERE (key = $1 and NOT deleted)", key).Scan(&mdsmeta)
	switch err {
	case sql.ErrNoRows:
		return "", storagedriver.PathNotFoundError{Path: key, DriverName: driverName}
	case nil:
		return mdsmeta.Key, nil
	default:
		return "", err
	}
}
