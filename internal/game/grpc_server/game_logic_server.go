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
	clog      *zap.Logger
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
	h, exist := common.GetNormalHandler(cmd)
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

// ForwardClientRoomMsg 接收gate 转发 房间客户端请求
func (s *GameLogicServer) ForwardClientRoomMsg(ctx context.Context, req *pb_service.ForwardReq) (*pb_service.ForwardRsp, error) {
	defer func() {
		if err := recover(); err != nil {
			stack := string(debug.Stack())
			s.clog.Error("ForwardClientMsg panic",
				zap.Any("err", err),
				zap.String("stack", stack),
			)
		}
	}()

	body := req.Req
	roomId := body.RoomId
	uid := body.Uid
	cmd := pb_room.CmdRoomKind(body.Cmd)
	payload := body.Payload

	// 校验房间归属
	if !s.rangeCalc.IsRoomBelong(roomId) {
		return &pb_service.ForwardRsp{
			Code: pb_base.ErrCode_EC_ERROR,
			Msg:  "room not belong this pod",
		}, nil
	}

	res := &pb_service.ForwardRsp{Code: pb_base.ErrCode_EC_OK}
	rInfo, exist := common.GetRoomHandler(cmd)
	if !exist {
		res.Code = pb_base.ErrCode_EC_ERROR
		res.Msg = "cmd not register"
		return res, nil
	}

	// 构造请求上下文
	tCtx := &common.TContext{
		Logger: s.clog.With(
			zap.Uint32("roomId", roomId),
			zap.Uint64("uid", uid),
			zap.Int32("cmd", int32(cmd)),
		),
		Uid:    uid,
		RoomId: roomId,
	}
	clog := tCtx.Logger
	clog.Debug("ForwardClientRoomMsg ", zap.Any("cmd", cmd))
	// 获取创建房间
	rm := s.roomMgr.GetOrCreateRoom(roomId)
	// 同步指令：直接阻塞执行，返回结果
	if rInfo.IsSync {
		b, err := rInfo.Handler(tCtx, payload, rm)
		if err != nil {
			clog.Error(err.Error())
			res.Code = pb_base.ErrCode_EC_ERROR
			res.Msg = err.Error()
			return res, nil
		}
		res.Body = &pb_base.RespBody{Payload: b}
		return res, nil
	}

	// 异步指令：丢入房间队列，立刻返回成功
	okSend := rm.SendMsg(tCtx, payload, rInfo)
	if !okSend {
		clog.Warn("SendMsg fail room queue full")
		res.Code = pb_base.ErrCode_EC_BUSY
		res.Msg = "room queue full"
		return res, nil
	}
	clog.Debug("ForwardClientRoomMsg ", zap.Any("cmd", cmd))
	return res, nil
}
