package mds

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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
}

// NewClient creates a client to MDS
func NewClient(config Config) (*Client, error) {
	return &Client{
		Config: config,
	}, nil
}

func (m *Client) uploadURL(namespace, filename string) string {
	return fmt.Sprintf("http://%s:%d/upload-%s/%s", m.Host, m.UploadPort, namespace, filename)
}

// ReadURL returns an URL which could be used to get data
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
func (m *Client) Upload(namespace string, filename string, body io.ReadCloser) (*UploadInfo, error) {
	urlStr := m.uploadURL(namespace, filename)
	req, err := http.NewRequest("POST", urlStr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusForbidden:
		return nil, fmt.Errorf("[%s] Update is prohibited for your namespace", resp.Status)
	case 507: // 507 Insufficient Storage
		return nil, fmt.Errorf("[%s] No space left in storage", resp.Status)
	case http.StatusOK:
	default:
		return nil, fmt.Errorf("[%s]", resp.Status)
	}

	var info UploadInfo
	if err := decodeXML(&info, resp.Body); err != nil {
		return nil, err
	}

	return &info, nil
}

// Get reads a given key from storage and return ReadCloser to body.
// User is repsonsible for closing returned ReadCloser
func (m *Client) Get(namespace, key string, Range ...uint64) (io.ReadCloser, error) {
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, nil
	case http.StatusNotFound:
		return nil, fmt.Errorf("[%s] No such key", resp.Status)
	case http.StatusGone, http.StatusNotAcceptable:
		return nil, fmt.Errorf("[%s] No such namespace", resp.Status)
	default:
		return nil, fmt.Errorf("[%s]", resp.Status)
	}
}

// GetFile like Get but returns bytes
func (m *Client) GetFile(namespace, key string, Range ...uint64) ([]byte, error) {
	output, err := m.Get(namespace, key, Range...)
	if err != nil {
		return nil, err
	}
	defer output.Close()

	return ioutil.ReadAll(output)
}

// Delete deletes key from na,espace
func (m *Client) Delete(namespace, key string) error {
	urlStr := m.deleteURL(namespace, key)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("[%s] No such key", resp.Status)
	default:
		return fmt.Errorf("[%s]", resp.Status)
	}
}

// Ping checks availability of proxy
func (m *Client) Ping() error {
	urlStr := m.pingURL()
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		return fmt.Errorf("[%s]", resp.Status)
	}
}

// DownloadInfo retrieves an information about direct link to a file
// if it's available
func (m *Client) DownloadInfo(namespace, key string) (*DownloadInfo, error) {
	urlStr := m.downloadinfoURL(namespace, key)

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", m.AuthHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusGone:
		return nil, fmt.Errorf("[%s] Seems DownloadInfo is disabled for the namesapce", resp.Status)
	case http.StatusNotFound:
		return nil, fmt.Errorf("[%s] No such key", resp.Status)
	case http.StatusOK:
	default:
		return nil, fmt.Errorf("[%s]", resp.Status)
	}

	var info DownloadInfo
	if err := decodeXML(&info, resp.Body); err != nil {
		return nil, err
	}

	return &info, nil
}
