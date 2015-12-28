package pgdriver

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

type inmemory struct {
	sync.Mutex
	data map[string][]byte
}

func newInMemory() (BinaryStorage, error) {
	return &inmemory{
		data: make(map[string][]byte),
	}, nil
}

func (i *inmemory) Store(data io.Reader) ([]byte, int64, error) {
	i.Lock()
	defer i.Unlock()

	keymeta := genKey()
	buff := new(bytes.Buffer)
	if _, err := io.Copy(buff, data); err != nil {
		return nil, 0, err
	}
	i.data[string(keymeta)] = buff.Bytes()
	return keymeta, int64(buff.Len()), nil
}

func (i *inmemory) Get(keymeta []byte, offset int64) (io.ReadCloser, error) {
	i.Lock()
	defer i.Unlock()

	data, ok := i.data[string(keymeta)]
	if !ok {
		return nil, fmt.Errorf("no such key: %s", keymeta)
	}

	if offset > 0 {
		data = data[offset:]
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (i *inmemory) Delete(keymeta []byte) error {
	i.Lock()
	defer i.Unlock()
	delete(i.data, string(keymeta))
	return nil
}
