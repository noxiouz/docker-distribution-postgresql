package pgdriver

import (
	"crypto/md5"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/docker/distribution/context"
)

type BinaryStorage interface {
	Store(ctx context.Context, data io.Reader) ([]byte, int64, error)
	Append(ctx context.Context, metakey []byte, data io.Reader, offset int64) (int64, error)
	Get(ctx context.Context, meta []byte, offset int64) (io.ReadCloser, error)
	Delete(ctx context.Context, meta []byte) error
}

// TODO: it is the most stupid implementation
func genKey() []byte {
	h := md5.New()
	io.CopyN(h, rand.Reader, 1024)
	return []byte(fmt.Sprintf("%x", h.Sum(nil)))
}

var (
	ErrInvalidOffset = errors.New("invalid offset for append")

	// ErrAppendUnsupported means that a BinaryStorage implementation
	// does not support write with non-zero offset (append)
	ErrAppendUnsupported = errors.New("append is not supported")
)
