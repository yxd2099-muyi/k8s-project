package common

import (
	pb_room "github.com/k8s/muyi/api/pb/room"
	"go.uber.org/zap"
	"sync"
)

type HandlerFunc func(ctx *TContext, req []byte) ([]byte, error)
type RouterInfo struct {
	IsSync  bool // 这个消息是否同步处理
	Handler HandlerFunc
}
type Router struct {
	clog    *zap.Logger
	handler sync.Map //key: pb.MessageType  value HandlerFunc
}

var roomRouter = &Router{}

func RegisterRoomHandler(cmdKind pb_room.CmdRoomKind, isHandleSync bool, fc HandlerFunc) {
	h := &RouterInfo{
		IsSync:  isHandleSync,
		Handler: fc,
	}
	roomRouter.handler.Store(cmdKind, h)
}

func GetRoomHandler(cmdKind pb_room.CmdRoomKind) (*RouterInfo, bool) {
	if handler, ok := roomRouter.handler.Load(cmdKind); ok {
		// 类型断言为 HandlerFunc
		if h, ok1 := handler.(*RouterInfo); ok1 {
			return h, true
		}
		return nil, false
	}
	return nil, false
}
