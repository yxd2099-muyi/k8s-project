package grpc_server

import (
	"context"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/game/k8s"
	"github.com/k8s/muyi/internal/game/push"
	"github.com/k8s/muyi/internal/game/room"
	//"github.com/k8s/muyi/internal/game/room"
	//"github.com/k8s/muyi/internal/game/k8s"
	//"github.com/k8s/muyi/internal/game/push"
)

type GameLogicServer struct {
	pb_service.UnimplementedGameLogicServer
	roomMgr   *room.RoomMgr
	rangeCalc *k8s.RoomRangeCalc
	pushMgr   *push.PushManager
}

func NewGameLogicServer(rm *room.RoomMgr, rc *k8s.RoomRangeCalc, pm *push.PushManager) *GameLogicServer {
	return &GameLogicServer{
		roomMgr:   rm,
		rangeCalc: rc,
		pushMgr:   pm,
	}
}

// ForwardClientMsg 接收gate转发的客户端请求
func (s *GameLogicServer) ForwardClientMsg(ctx context.Context, req *pb_service.ForwardReq) (*pb_service.ForwardRsp, error) {
	body := req.Req
	roomId := body.RoomId
	//uid := body.Uid

	// 校验房间归属
	if !s.rangeCalc.IsRoomBelong(roomId) {
		return &pb_service.ForwardRsp{
			Code: pb_base.ErrCode_EC_ROOM_NOT_EXIST,
			Msg:  "room not belong this pod",
		}, nil
	}

	// 业务cmd分发
	switch body.Cmd {
	case uint32(pb_base.CmdKind_CMD_CREATE_ROOM):
		code := s.roomMgr.CreateRoom(roomId)
		return &pb_service.ForwardRsp{
			Code: code,
			Msg:  "success hello yang",
			Body: &pb_base.RespBody{
				Cmd:    body.Cmd,
				RoomId: roomId,
			},
		}, nil
	default:
		return &pb_service.ForwardRsp{
			Code: pb_base.ErrCode_EC_PARAM_INVALID,
			Msg:  "unknown cmd",
		}, nil
	}
}
