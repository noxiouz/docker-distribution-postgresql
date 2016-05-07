package pgdriver

import (
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
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

// TODO: it is the most stupid implementation
func genKey() []byte {
	h := md5.New()
	io.CopyN(h, rand.Reader, 1024)
	return []byte(fmt.Sprintf("%x", h.Sum(nil)))
}

var (
	// ErrAppendUnsupported means that a BinaryStorage implementation
	// does not support write with non-zero offset (append)
	ErrAppendUnsupported = errors.New("append is not supported")
)
