package pgdriver

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"

	"github.com/docker/distribution/context"
)

type inmemory struct {
	sync.Mutex
	baseURL string
	data    map[string][]byte
}

func newInMemory() (KVStorage, error) {
	// NOTE: distribution does not require any kind of Close method,
	// so there is no possibility to prevent resourse leak
	driver := &inmemory{
		data: make(map[string][]byte),
	}
	ts := httptest.NewServer(http.HandlerFunc(driver.serve))

	driver.baseURL = ts.URL

	return driver, nil
}

func (i *inmemory) serve(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	i.Lock()
	defer i.Unlock()
	data, ok := i.data[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (i *inmemory) Store(ctx context.Context, key string, data io.Reader) (int64, error) {
	i.Lock()
	defer i.Unlock()

	buff := new(bytes.Buffer)
	if _, err := io.Copy(buff, data); err != nil {
		return 0, err
	}
	i.data[key] = buff.Bytes()
	return int64(buff.Len()), nil
}

func (i *inmemory) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	i.Lock()
	defer i.Unlock()

	data, ok := i.data[key]
	if !ok {
		return nil, fmt.Errorf("no such key: %s", key)
	}

	if int64(len(data)) < offset {
		return nil, fmt.Errorf("invalid offset")
	}

	if offset > 0 {
		data = data[offset:]
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

// func (i *inmemory) Size(ctx context.Context, key string) (int64, error) {
// 	i.Lock()
// 	defer i.Unlock()
//
// 	data, ok := i.data[key]
// 	if !ok {
// 		return 0, fmt.Errorf("no such key: %s", key)
// 	}
// 	return int64(len(data)), nil
// }

func (i *inmemory) Delete(ctx context.Context, key string) error {
	i.Lock()
	defer i.Unlock()
	delete(i.data, key)
	return nil
}

func (i *inmemory) Append(ctx context.Context, key string, data io.Reader) (int64, error) {
	i.Lock()
	defer i.Unlock()

	body, ok := i.data[key]
	if !ok {
		return 0, fmt.Errorf("EINVAL OFFSET. NO SUCH FILE %s", key)
	}

	buff := new(bytes.Buffer)
	nn, err := io.Copy(buff, data)
	if err != nil {
		return nn, err
	}

	i.data[key] = append(body, buff.Bytes()...)
	return nn, nil
}

func (i *inmemory) URLFor(ctx context.Context, key string, _ bool) (string, error) {
	u, err := url.Parse(i.baseURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
