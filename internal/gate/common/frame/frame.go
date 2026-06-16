package frame

import (
	pb_base "github.com/k8s/muyi/api/pb/base"
	"google.golang.org/protobuf/proto"
)

// EncodeWsFrame 序列化外层ws帧
func EncodeWsFrame(f *pb_base.WsFrame) ([]byte, error) {
	return proto.Marshal(f)
}

// DecodeWsFrame 反序列化ws帧
func DecodeWsFrame(data []byte) (*pb_base.WsFrame, error) {
	var frame pb_base.WsFrame
	if err := proto.Unmarshal(data, &frame); err != nil {
		return nil, err
	}
	return &frame, nil
}
