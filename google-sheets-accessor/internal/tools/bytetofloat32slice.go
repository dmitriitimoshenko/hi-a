package tools

import (
	"bytes"
	"encoding/json"
)

func ByteToFloat32Slice(b []byte) ([]float32, error) {
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return nil, nil
	}

	data := b

	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}
		data = []byte(s)
	}

	var v []float32
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	return v, nil
}
