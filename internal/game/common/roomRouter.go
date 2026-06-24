package common

import (
	pb_room "github.com/k8s/muyi/api/pb/room"
	"go.uber.org/zap"
	"sync"
)

type RoomHandlerFunc func(ctx *TContext, req []byte, room IRoom) ([]byte, error)
type RoomRouterInfo struct {
	IsSync  bool // 这个消息是否同步处理
	Handler RoomHandlerFunc
}
type RoomRouter struct {
	clog    *zap.Logger
	handler sync.Map //key: pb.MessageType  value HandlerFunc
}

var roomRouter = &RoomRouter{}

func RegisterRoomHandler(cmdKind pb_room.CmdRoomKind, isHandleSync bool, fc RoomHandlerFunc) {
	h := &RoomRouterInfo{
		IsSync:  isHandleSync,
		Handler: fc,
	}
	roomRouter.handler.Store(cmdKind, h)
}

func GetRoomHandler(cmdKind pb_room.CmdRoomKind) (*RoomRouterInfo, bool) {
	if handler, ok := roomRouter.handler.Load(cmdKind); ok {
		// 类型断言为 HandlerFunc
		if h, ok1 := handler.(*RoomRouterInfo); ok1 {
			return h, true
		}
		return nil, false
	}
	return nil, false
}
