package grpc_server

import (
	"context"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/hub"
	"github.com/k8s/muyi/shared/kit/serializer"
)

type GatePushServer struct {
	pb_service.UnimplementedGatePushServer
	hub *hub.Hub
}

func NewGatePushServer(h *hub.Hub) *GatePushServer {
	return &GatePushServer{hub: h}
}

// Push GameServer主动推送单用户/批量广播
func (s *GatePushServer) Push(ctx context.Context, req *pb_service.PushReq) (*pb_service.PushResp, error) {
	data, err := serializer.EncodeProto(req.Body)
	if err != nil {
		return &pb_service.PushResp{Code: int32(pb_base.ErrCode_EC_INTERNAL_ERR)}, nil
	}
	// 批量推送
	s.hub.BatchPush(req.Uids, data)
	return &pb_service.PushResp{Code: int32(pb_base.ErrCode_EC_OK)}, nil
}
