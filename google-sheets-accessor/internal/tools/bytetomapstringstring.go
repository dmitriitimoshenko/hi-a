package tools

import (
	"encoding/json"
)

func ByteToMapStringString(b []byte) (*map[string]string, error) {
	var meta map[string]string
	if len(b) > 0 {
		if err := json.Unmarshal(b, &meta); err != nil {
			return nil, err
		}
	}

	return &meta, nil
}
