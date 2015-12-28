package pgdriver

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
)

type BinaryStorage interface {
	Store(data io.Reader) ([]byte, int64, error)
	Get(meta []byte, offset int64) (io.ReadCloser, error)
	Delete(meta []byte) error
}

// TODO: it is the most stupid implementation
func genKey() []byte {
	h := md5.New()
	io.CopyN(h, rand.Reader, 1024)
	return []byte(fmt.Sprintf("%x", h.Sum(nil)))
}
