package pgdriver

import (
	"io"

	"github.com/docker/distribution/context"
)

// KVStorage is an abstraction on top of any Key/Value storage
type KVStorage interface {
	Store(ctx context.Context, key string, data io.Reader) (int64, error)
	Append(ctx context.Context, key string, data io.Reader) (int64, error)
	Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	URLFor(ctx context.Context, key string) (string, error)
}
