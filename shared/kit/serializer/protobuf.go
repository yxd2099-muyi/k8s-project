package serializer

import (
	"google.golang.org/protobuf/proto"
)

func EncodeProto(f proto.Message) ([]byte, error) {
	return proto.Marshal(f)
}
func DecodeProto(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}
