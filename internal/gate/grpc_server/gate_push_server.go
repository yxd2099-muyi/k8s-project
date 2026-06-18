package grpc_server

import (
	"context"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/conn"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"time"
)

type GatePushServer struct {
	pb_service.UnimplementedGatePushServer
	hub  *conn.ConnManager
	clog *zap.Logger
}

func NewGatePushServer(h *conn.ConnManager) *GatePushServer {
	return &GatePushServer{hub: h, clog: logger.L}
}

// Push GameServer主动推送单用户/批量广播
func (s *GatePushServer) Push(ctx context.Context, req *pb_service.PushReq) (*pb_service.PushResp, error) {
	bodyData, err := serializer.EncodeProto(req.GetBody())
	if err != nil {
		return &pb_service.PushResp{Code: int32(pb_base.ErrCode_EC_ERROR)}, nil
	}

	respFrame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_PUSH,
		FirstKind: pb_base.FirstKind_FIRST_PUSH,
		Payload:   bodyData,
		Timestamp: time.Now().Unix(),
	}
	s.clog.Debug("[GatePushServer] Push frame", zap.Any("frame", respFrame))
	data, err := serializer.EncodeProto(respFrame)
	if err != nil {
		return &pb_service.PushResp{Code: int32(pb_base.ErrCode_EC_ERROR)}, nil
	}
	// 批量推送
	s.hub.BatchPush(req.GetUids(), data)
	return &pb_service.PushResp{Code: int32(pb_base.ErrCode_EC_OK)}, nil
}
