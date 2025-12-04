package tools

import "gorm.io/datatypes"

func ToJSONMap(src map[string]string) *datatypes.JSONMap {
	jm := datatypes.JSONMap{}
	for k, v := range src {
		jm[k] = v
	}
	return &jm
}
