package tools

import (
	"fmt"

	"gorm.io/datatypes"
)

func FromJSONMap(src *datatypes.JSONMap) map[string]string {
	if src == nil {
		return nil
	}

	dst := make(map[string]string, len(*src))
	for k, v := range *src {
		if s, ok := v.(string); ok {
			dst[k] = s
			continue
		}

		if v != nil {
			dst[k] = fmt.Sprint(v)
		}
	}

	return dst
}
