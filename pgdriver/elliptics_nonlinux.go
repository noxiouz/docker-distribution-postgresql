// +build !linux

package pgdriver

import (
	"fmt"
)

func newEllipticsKVStorage(parameters map[string]interface{}) (KVStorage, error) {
	return nil, fmt.Errorf("Elliptics driver is not available in non Linux builds")
}
