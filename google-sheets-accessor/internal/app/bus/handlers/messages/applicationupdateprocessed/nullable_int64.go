package applicationupdateprocessed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

type NullableInt64 struct {
	value *int64
}

func (n NullableInt64) Ptr() *int64 {
	return n.value
}

func (n NullableInt64) IsZero() bool {
	return n.value == nil
}

func (n NullableInt64) MarshalJSON() ([]byte, error) {
	if n.value == nil {
		return []byte("null"), nil
	}

	return json.Marshal(*n.value)
}

func (n *NullableInt64) UnmarshalJSON(data []byte) error {
	cleanData := bytes.TrimSpace(data)
	if len(cleanData) == 0 || bytes.Equal(cleanData, []byte("null")) {
		n.value = nil
		return nil
	}

	var asInt int64
	if err := json.Unmarshal(cleanData, &asInt); err == nil {
		n.value = &asInt
		return nil
	}

	var asString string
	if err := json.Unmarshal(cleanData, &asString); err == nil {
		if asString == "" {
			n.value = nil
			return nil
		}

		parsed, err := strconv.ParseInt(asString, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse int64 from string %q: %w", asString, err)
		}

		n.value = &parsed
		return nil
	}

	return fmt.Errorf("expected int64 or numeric string, got %s", string(cleanData))
}
