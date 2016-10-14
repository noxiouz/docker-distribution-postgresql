package mds

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// TODO: there are lots of memory allocations

// ErrorMethodScope is a scope of a failed operation
type ErrorMethodScope struct {
	Method string
	URL    string
}

// ErrorResponseScope contains information about a http reply
type ErrorResponseScope struct {
	Status string
	Body   []byte
}

func (err ErrorResponseScope) String() string {
	return fmt.Sprintf("%s %s", err.Status, err.Body)
}

func newResponseScope(resp *http.Response) ErrorResponseScope {
	var buff = new(bytes.Buffer)
	// we really do not care about any error here
	io.CopyN(buff, resp.Body, 512)
	return ErrorResponseScope{
		Status: resp.Status,
		Body:   buff.Bytes(),
	}
}

// MethodError wraps http replies from MDS to provide convenient info about errors
type MethodError struct {
	ErrorMethodScope
	ErrorResponseScope
}

func (err MethodError) Error() string {
	return fmt.Sprintf("%s failed on %s: %s", err.Method, err.URL, err.ErrorResponseScope.String())
}

func newMethodError(scope ErrorMethodScope, resp *http.Response) error {
	err := MethodError{
		ErrorMethodScope:   scope,
		ErrorResponseScope: newResponseScope(resp),
	}
	return err
}
