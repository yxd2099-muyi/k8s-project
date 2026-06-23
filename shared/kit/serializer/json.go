package serializer

import (
	"encoding/json"
)

func EncodeJson(v any) ([]byte, error) {
	return json.Marshal(v)
}
func DecodeJson(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	return err
}
func DecodeJsonForString(data string, v any) error {
	err := json.Unmarshal([]byte(data), v)
	return err
}
