package mds

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"golang.org/x/net/context"
)

// UploadInfo describes result of upload
type UploadInfo struct {
	XMLName xml.Name `xml:"post"`
	Obj     string   `xml:"obj,attr"`
	ID      string   `xml:"id,attr"`
	Key     string   `xml:"key,attr"`
	Size    uint64   `xml:"size,attr"`
	Groups  int      `xml:"groups,attr"`

	Complete []struct {
		Addr   string `xml:"addr,attr"`
		Path   string `xml:"path,attr"`
		Group  int    `xml:"group,attr"`
		Status int    `xml:"status,attr"`
	} `xml:"complete"`

	Written int `xml:"written"`
}

func decodeXML(result interface{}, body io.Reader) error {
	return xml.NewDecoder(body).Decode(result)
}

// DownloadInfo describes a direct link to a file
type DownloadInfo struct {
	XMLName xml.Name `xml:"download-info"`
	Host    string   `xml:"host"`
	Path    string   `xml:"path"`
	TS      string   `xml:"ts"`
	Region  int      `xml:"region"`
	Sign    string   `xml:"s"`
}

// URL constructs a direct link from DownloadInfo
func (d *DownloadInfo) URL() string {
	return fmt.Sprintf("http://%s%s?ts=%ssign=%s", d.Host, d.Path, d.TS, d.Sign)
}

// Config represents configuration for the client
type Config struct {
	Host       string
	UploadPort int
	ReadPort   int

	AuthHeader string
}

// Client works with MDS
type Client struct {
	Config

	client *http.Client
}

// NewClient creates a client to MDS
func NewClient(config Config, client *http.Client) (*Client, error) {
	if client == nil {
		client = http.DefaultClient
	}

	return &Client{
		Config: config,

		client: client,
	}, nil
}

func (m *Client) uploadURL(namespace, filename string) string {
	return fmt.Sprintf("http://%s:%d/upload-%s/%s", m.Host, m.UploadPort, namespace, filename)
}

// ReadURL returns a URL which could be used to get data.
func (m *Client) ReadURL(namespace, filename string) string {
	return fmt.Sprintf("http://%s:%d/get-%s/%s", m.Host, m.ReadPort, namespace, filename)
}

func (m *Client) deleteURL(namespace, filename string) string {
	return fmt.Sprintf("http://%s:%d/delete-%s/%s", m.Host, m.UploadPort, namespace, filename)
}

func (m *Client) pingURL() string {
	return fmt.Sprintf("http://%s:%d/ping", m.Host, m.ReadPort)
}

func (m *Client) downloadinfoURL(namespace, filename string) string {
	return fmt.Sprintf("http://%s:%d/downloadinfo-%s/%s", m.Host, m.ReadPort, namespace, filename)
}

// Upload stores provided data to a specified namespace. Returns information about upload.
func (m *Client) Upload(ctx context.Context, namespace string, filename string, size int64, body io.Reader) (*UploadInfo, error) {
	urlStr := m.uploadURL(namespace, filename)
	req, err := http.NewRequest("POST", urlStr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", m.AuthHeader)
	if req.ContentLength <= 0 {
		req.ContentLength = size
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		scope := ErrorMethodScope{
			Method: "upload",
			URL:    urlStr,
		}
		return nil, newMethodError(scope, resp)
	}

	var info UploadInfo
	if err := decodeXML(&info, resp.Body); err != nil {
		return nil, err
	}

	return &info, nil
}

// Get reads a given key from storage and return ReadCloser to body.
// User is responsible for closing returned ReadCloser.
func (m *Client) Get(ctx context.Context, namespace, key string, Range ...uint64) (io.ReadCloser, error) {
	urlStr := m.ReadURL(namespace, key)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	switch len(Range) {
	case 0:
	case 1:
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-", Range[0]))
	case 2:
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", Range[0], Range[1]))
	default:
		return nil, fmt.Errorf("Invalid range")
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		return resp.Body, nil
	}

	defer resp.Body.Close()
	scope := ErrorMethodScope{
		Method: "get",
		URL:    urlStr,
	}
	return nil, newMethodError(scope, resp)
}

// GetFile is like Get but returns bytes.
func (m *Client) GetFile(ctx context.Context, namespace, key string, Range ...uint64) ([]byte, error) {
	output, err := m.Get(ctx, namespace, key, Range...)
	if err != nil {
		return nil, err
	}
	defer output.Close()

	return ioutil.ReadAll(output)
}

// Delete deletes key from namespace.
func (m *Client) Delete(ctx context.Context, namespace, key string) error {
	urlStr := m.deleteURL(namespace, key)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		scope := ErrorMethodScope{
			Method: "delete",
			URL:    urlStr,
		}
		return newMethodError(scope, resp)
	}

	return nil
}

// Ping checks availability of proxy
func (m *Client) Ping(ctx context.Context) error {
	urlStr := m.pingURL()
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", m.AuthHeader)
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		scope := ErrorMethodScope{
			Method: "ping",
			URL:    urlStr,
		}
		return newMethodError(scope, resp)
	}
	return nil
}

// DownloadInfo retrieves an information about direct link to a file,
// if it's available.
func (m *Client) DownloadInfo(ctx context.Context, namespace, key string) (*DownloadInfo, error) {
	urlStr := m.downloadinfoURL(namespace, key)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		scope := ErrorMethodScope{
			Method: "downloadInfo",
			URL:    urlStr,
		}
		return nil, newMethodError(scope, resp)
	}

	var info DownloadInfo
	if err := decodeXML(&info, resp.Body); err != nil {
		return nil, err
	}

	return &info, nil
}
