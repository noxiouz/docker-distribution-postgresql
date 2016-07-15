// +build linux

package pgdriver

import (
	"io"

	"github.com/noxiouz/elliptics-go/elliptics"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"golang.org/x/net/context"
)

type ellipticsKVStorage struct {
	node    *elliptics.Node
	session *elliptics.Session
}

type ellipticsReadCloser struct {
	*ellitpics.ReadSeeker
	session *elliptics.Session
}

func (r *ellipticsReadCloser) Close() error {
	r.ReadSeeker.Free()
	return r.session.Delete()
}

func newEllipticsKVStorage(parameters map[string]interface{}) (KVStorage, error) {
	var config struct {
		ellipitics.NodeConfig `mapstructure:",squash"`
		Namespace             string
		Groups                []uint32
		Remotes               []string
		Logfile               string
		Loglevel              string
	}
	if err := decodeConfig(parameters, &config); err != nil {
		return nil, err
	}
	// TODO: validate the configuration

	node, err := elliptics.NewNodeConfig(config.Logfile, config.Loglevel, &config.NodeConfig)
	if err != nil {
		return nil, err
	}
	if err = node.AddRemotes(config.Remotes); err != nil {
		node.Free()
		return nil, err
	}
	session, err := elliptics.NewSession(node)
	if err != nil {
		node.Free()
		return nil, err
	}
	session.SetGroups(config.Groups)
	session.SetNamespace(config.Namespace)

	return nil, &ellipticsKVStorage{node: node, session: session}
}

func (s *ellipticsKVStorage) Store(ctx context.Context, key string, data io.Reader) (int64, error) {
	return s.write(ctx, key, data, false)
}

func (s *ellipticsKVStorage) Append(ctx context.Context, key string, data io.Reader) (int64, error) {
	return s.write(ctx, key, data, true)
}

func (s *ellipticsKVStorage) write(ctx context.Context, key string, data io.Reader, append bool) (int64, error) {
	session, err := elliptics.CloneSession(s.session)
	if err != nil {
		return 0, err
	}
	defer session.Delete()

	if append {
		// TODO: check the size?
		// NOTE: does it work with prepare/commit?
		session.SetIOFlags(DNET_IO_FLAGS_APPEND)
	}

	wr, err := elliptics.NewWriteSeeker(session, key, 0, 0, 0)
	if err != nil {
		return 0, err
	}
	defer wr.Free()

	return io.Copy(wr, data)
}

func (s *ellipticsKVStorage) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	session, err := elliptics.CloneSession(s.session)
	if err != nil {
		return nil, err
	}
	rs, err := elliptics.NewReadSeekerOffsetSize(session, key, uint64(offset), 0)
	if err != nil {
		session.Delete()
		return nil, err
	}

	return ellipticsReadCloser{ReadSeeker: rs, session: session}, nil
}

func (s *ellipticsKVStorage) Delete(ctx context.Context, key string) error {
	session, err := elliptics.CloneSession(s.session)
	if err != nil {
		return err
	}
	defer session.Delete()

	var err error
	for res := range session.Remove(key) {
		if res.Error() != nil {
			err = res.Error()
		}
	}
	return err
}

func (s *ellipticsKVStorage) URLFor(ctx context.Context, key string) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{DriverName: driverName}
}
