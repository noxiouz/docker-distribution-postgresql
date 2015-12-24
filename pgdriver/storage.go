package pgdriver

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

type BinaryStorage interface {
	Store(data io.Reader) (key string, nn int64, err error)
	Get(key string, offset int64) (io.ReadCloser, error)
	// Delete(key string) error
}

type inmemory struct {
	sync.Mutex
	data map[string][]byte
}

func genKey() string {
	h := md5.New()
	io.CopyN(h, rand.Reader, 1024)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func newInMemory() (BinaryStorage, error) {
	return &inmemory{
		data: make(map[string][]byte),
	}, nil
}

func (i *inmemory) Store(data io.Reader) (string, int64, error) {
	i.Lock()
	defer i.Unlock()

	key := genKey()
	buff := new(bytes.Buffer)
	if _, err := io.Copy(buff, data); err != nil {
		return "", 0, err
	}
	i.data[key] = buff.Bytes()
	return key, int64(buff.Len()), nil
}

func (i *inmemory) Get(key string, offset int64) (io.ReadCloser, error) {
	i.Lock()
	defer i.Unlock()

	data, ok := i.data[key]
	if !ok {
		return nil, fmt.Errorf("no such key: %s", key)
	}

	if offset > 0 {
		data = data[offset:]
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}
