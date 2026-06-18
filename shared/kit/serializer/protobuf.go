package serializer

import (
	pb_base "github.com/k8s/muyi/api/pb/base"
	"google.golang.org/protobuf/proto"
)

func EncodeProto(f proto.Message) ([]byte, error) {
	return proto.Marshal(f)
}
func DecodeProto(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}

// DecodeWsFrame 反序列化ws帧
func DecodeWsFrame(data []byte) (*pb_base.WsFrame, error) {
	var frame pb_base.WsFrame
	if err := proto.Unmarshal(data, &frame); err != nil {
		return nil, err
	}
	return &frame, nil
}
