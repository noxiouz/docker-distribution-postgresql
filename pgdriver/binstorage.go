package pgdriver

import (
	"errors"
	"io"

	"github.com/docker/distribution/context"
)

type KVStorage interface {
	Store(ctx context.Context, key string, data io.Reader) (int64, error)
	Append(ctx context.Context, key string, data io.Reader) (int64, error)
	Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	URLFor(ctx context.Context, key string) (string, error)
}

var (
	// ErrAppendUnsupported means that a BinaryStorage implementation
	// does not support write with non-zero offset (append)
	ErrAppendUnsupported = errors.New("append is not supported")
)
