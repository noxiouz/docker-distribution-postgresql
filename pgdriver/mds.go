package pgdriver

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/docker/distribution/context"

	"github.com/noxiouz/mds"
)

type metaInfo struct {
	Key  string
	Size int64
	ID   string
}

type mdsBinStorage struct {
	Storage   *mds.Client
	Namespace string
}

func newMDSBinStorage(parameters map[string]interface{}) (BinaryStorage, error) {
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
		Storage:   mdsClient,
		Namespace: config.Namespace,
	}, nil
}

func (m *mdsBinStorage) Store(ctx context.Context, data io.Reader) ([]byte, int64, error) {
	key := genKey()
	uinfo, err := m.Storage.Upload(m.Namespace, string(key), ioutil.NopCloser(data))
	if err != nil {
		return nil, 0, err
	}

	var meta = metaInfo{
		Key:  uinfo.Key,
		Size: int64(uinfo.Size),
		ID:   uinfo.ID,
	}

	metakey, err := json.Marshal(meta)
	if err != nil {
		return nil, 0, err
	}
	return metakey, meta.Size, nil
}

func (m *mdsBinStorage) Get(ctx context.Context, metakey []byte, offset int64) (io.ReadCloser, error) {
	var mdsmeta metaInfo
	if err := json.Unmarshal(metakey, &mdsmeta); err != nil {
		return nil, err
	}

	return m.Storage.Get(m.Namespace, mdsmeta.Key, uint64(offset))
}

func (m *mdsBinStorage) Delete(ctx context.Context, meta []byte) error {
	return fmt.Errorf("Delete is not implemneted in MDS")
}

func (m *mdsBinStorage) Append(ctx context.Context, metakey []byte, data io.Reader, offset int64) (int64, error) {
	return 0, ErrAppendUnsupported
}
