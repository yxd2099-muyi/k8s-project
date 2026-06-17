package grpc_server

import (
	"context"
	"fmt"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_room "github.com/k8s/muyi/api/pb/room"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/internal/router"
	"github.com/k8s/muyi/internal/game/k8s"
	"github.com/k8s/muyi/internal/game/room"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"runtime/debug"
)

type GameLogicServer struct {
	pb_service.UnimplementedGameLogicServer
	roomMgr   *room.RoomMgr
	rangeCalc *k8s.RoomRangeCalc
	//pushMgr   *push.PushManager
	clog *zap.Logger
}

func NewGameLogicServer(rm *room.RoomMgr, rc *k8s.RoomRangeCalc) *GameLogicServer {
	router.InitRouter()
	s := &GameLogicServer{
		roomMgr:   rm,
		rangeCalc: rc,
		clog:      logger.L,
	}
	return s
}

// ForwardClientMsg 接收gate转发的客户端请求
func (s *GameLogicServer) ForwardClientMsg(ctx context.Context, req *pb_service.ForwardReq) (*pb_service.ForwardRsp, error) {
	defer func() {
		if err := recover(); err != nil {
			stack := string(debug.Stack())
			s.clog.Error("任务处理panic makeProcessTaskSafe",
				zap.Any("err", err),
				zap.String("stack", stack), // 关键
			)
		}
	}()
	body := req.Req
	roomId := body.RoomId
	//uid := body.Uid
	cmd := pb_room.CmdRoomKind(body.Cmd)
	payload := body.Payload
	// 校验房间归属
	if !s.rangeCalc.IsRoomBelong(roomId) {
		return &pb_service.ForwardRsp{
			Code: pb_base.ErrCode_EC_ERROR,
			Msg:  "room not belong this pod",
		}, nil
	}
	res := &pb_service.ForwardRsp{}
	res.Code = pb_base.ErrCode_EC_OK
	h, exist := common.GetRoomHandler(cmd)
	s.clog.Debug("ForwardClientMsg ", zap.Any("cmd", cmd))
	if !exist {
		res.Code = pb_base.ErrCode_EC_ERROR
		res.Msg = "cmd not exist"
		return res, nil
	}
	tCtx, ok := ctx.Value(common.TContextKey{}).(*common.TContext)
	clog := tCtx.Logger
	if !ok {
		text := fmt.Sprintf("handle context not exist")
		res.Msg = text
		clog.Error(text)
		return res, nil
	}
	tCtx.Logger = clog.With(zap.Any("cmd", cmd))
	// 业务处理
	b, err := h.Handler(tCtx, payload)
	if err != nil {
		res.Code = pb_base.ErrCode_EC_ERROR
		res.Msg = err.Error()
		return res, nil
	}
	resB := &pb_base.RespBody{}
	resB.Payload = b
	res.Body = resB
	res.Msg = "success"
	return res, nil
}
