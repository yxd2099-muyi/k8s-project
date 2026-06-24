package common

import (
	pb_room "github.com/k8s/muyi/api/pb/room"
	"go.uber.org/zap"
	"sync"
)

type NormalHandlerFunc func(ctx *TContext, req []byte) ([]byte, error)
type NormalRouterInfo struct {
	IsSync  bool // 这个消息是否同步处理
	Handler NormalHandlerFunc
}
type NormalRouter struct {
	clog    *zap.Logger
	handler sync.Map //key: pb.MessageType  value HandlerFunc
}

var normalRouter = &NormalRouter{}

func RegisterNormalHandler(cmdKind pb_room.CmdRoomKind, isHandleSync bool, fc NormalHandlerFunc) {
	h := &NormalRouterInfo{
		IsSync:  isHandleSync,
		Handler: fc,
	}
	normalRouter.handler.Store(cmdKind, h)
}

func GetNormalHandler(cmdKind pb_room.CmdRoomKind) (*NormalRouterInfo, bool) {
	if handler, ok := normalRouter.handler.Load(cmdKind); ok {
		// 类型断言为 HandlerFunc
		if h, ok1 := handler.(*NormalRouterInfo); ok1 {
			return h, true
		}
		return nil, false
	}
	return nil, false
}
