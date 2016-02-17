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

func (i *inmemory) Append(metakey []byte, data io.Reader, offset int64) (int64, error) {
	i.Lock()
	defer i.Unlock()

	body, ok := i.data[string(metakey)]
	if !ok {
		return 0, fmt.Errorf("EINVAL OFFSET. NO SUCH FILE %s", metakey)
	}

	buff := new(bytes.Buffer)
	nn, err := io.Copy(buff, data)
	if err != nil {
		return nn, err
	}

	if offset > int64(len(body)) {
		var extended = make([]byte, offset+nn)
		copy(extended, body)
		extended = append(extended[:offset], buff.Bytes()...)
		i.data[string(metakey)] = extended
	} else {
		i.data[string(metakey)] = append(body[:offset], buff.Bytes()...)
	}
	// fmt.Printf("%d %d %d\n", offset, nn, len(body))
	return nn, nil
}
