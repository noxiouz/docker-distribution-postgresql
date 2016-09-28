package pgdriver

import (
	"bytes"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"time"

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

	tr := &http.Transport{
		// TODO: make it configurable
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{
				DualStack: true,
				Timeout:   time.Second * 3,
			}
			return d.Dial(network, addr)
		},
		// This value is set according to the current amount of DB Idle conns
		MaxIdleConnsPerHost: 10,
	}

	mdsClient, err := mds.NewClient(config.Config, &http.Client{Transport: tr})
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
	return m.store(ctx, key, data, getContentLength(ctx))
}

func (m *mdsBinStorage) store(ctx context.Context, key string, data io.Reader, size int64) (int64, error) {
	uinfo, err := m.Storage.Upload(ctx, m.Namespace, key, size, ioutil.NopCloser(data))
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
		if mdserr := m.Storage.Delete(ctx, m.Namespace, uinfo.Key); mdserr != nil {
			context.GetLoggerWithFields(ctx, map[interface{}]interface{}{"error": mdserr, "key": uinfo.Key}).Error("can not clean MDS after DB error")
		}
		return 0, err
	}

	return meta.Size, nil
}

func (m *mdsBinStorage) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	metainfo, err := m.getMDSMetaInfo(ctx, key)
	if err != nil {
		return nil, err
	}

	if offset >= metainfo.Size {
		return ioutil.NopCloser(bytes.NewReader(make([]byte, 0))), nil
	}

	return m.Storage.Get(ctx, m.Namespace, metainfo.Key, uint64(offset))
}

func (m *mdsBinStorage) Delete(ctx context.Context, key string) error {
	metainfo, err := m.getMDSMetaInfo(ctx, key)
	if err != nil {
		return err
	}

	if err = m.Storage.Delete(ctx, m.Namespace, metainfo.Key); err != nil {
		return err
	}

	// Mark deleted
	_, err = m.DB(pgcluster.MASTER).Exec("UPDATE mds SET deleted = true WHERE (key = $1)", key)
	if err != nil {
		context.GetLogger(ctx).Errorf("update metainfo about deleted key %s error: %v", key, err)
		return err
	}

	return nil
}

func (m *mdsBinStorage) Append(ctx context.Context, key string, data io.Reader) (int64, error) {
	metainfo, err := m.getMDSMetaInfo(ctx, key)
	switch err.(type) {
	case storagedriver.PathNotFoundError:
		return m.Store(ctx, key, data)
	case nil:
		var body []byte
		size := getContentLength(ctx)
		// NOTE: Append to a file is NOT expected to be used in MDS,
		// but noresumable tag does not work in distribution
		context.GetLogger(ctx).Warnf("Append via Read/Delete is ineffective in MDS: %d %s %v", size, key, metainfo)
		body, err = m.Storage.GetFile(ctx, m.Namespace, metainfo.Key)
		if err != nil {
			context.GetLogger(ctx).Errorf("Unable to read MDS File %s: %v", metainfo.Key, err)
			return 0, err
		}

		// In case we have no request in a context
		if size > 0 {
			size += int64(len(body))
		}

		mr := io.MultiReader(bytes.NewReader(body), data)
		if err = m.Storage.Delete(ctx, m.Namespace, metainfo.Key); err != nil {
			context.GetLogger(ctx).Errorf("Unable to delete from MDS %s: %v", metainfo.Key, err)
			return 0, err
		}

		_, err = m.DB(pgcluster.MASTER).Exec("DELETE FROM mds WHERE (key = $1)", key)
		if err != nil {
			context.GetLogger(ctx).Errorf("delete metainfo about key %s error: %v", key, err)
		}

		return m.store(ctx, key, mr, size)
	default:
		return 0, err
	}
}

func (m *mdsBinStorage) URLFor(ctx context.Context, key string) (string, error) {
	metainfo, err := m.getMDSMetaInfo(ctx, key)
	if err != nil {
		return "", err
	}

	return m.Storage.ReadURL(m.Namespace, metainfo.Key), nil
}

func (m *mdsBinStorage) getMDSMetaInfo(ctx context.Context, key string) (*metaInfo, error) {
	var mdsmeta metaInfo
	err := m.DB(pgcluster.MASTER).QueryRow("SELECT mdsfileinfo FROM mds WHERE (key = $1 and NOT deleted)", key).Scan(&mdsmeta)
	switch err {
	case sql.ErrNoRows:
		return nil, storagedriver.PathNotFoundError{Path: key, DriverName: driverName}
	case nil:
		return &mdsmeta, nil
	default:
		return nil, err
	}
}

func getContentLength(ctx context.Context) int64 {
	req, err := context.GetRequest(ctx)
	if err != nil {
		context.GetLogger(ctx).Warnf("unable to find out ContentLength: %v", err)
		return 0
	}
	context.GetLogger(ctx).Infof("request.ContentLength: %d", req.ContentLength)
	return req.ContentLength
}
