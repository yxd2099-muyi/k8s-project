package common

import (
	"context"
	pb_push "github.com/k8s/muyi/api/pb/push"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type IRoom interface {
	RoomID() uint32
	// 推送相关
	Broadcast(clog *zap.Logger, cmd pb_push.CmdPushKind, p proto.Message)
	SinglePush(clog *zap.Logger, uid uint64, cmd pb_push.CmdPushKind, p proto.Message)
	// 玩家数据
	GetAllPlayerUids() []uint64
	GetPlayer(uid uint64) any
	AddPlayer(uid uint64)
	DelPlayer(uid uint64)
	SendMsg(tCtx *TContext, payload []byte, info *RoomRouterInfo) bool
	// 投递房间业务事件
	PushEvent(evt RoomEvent) bool
}

// RoomEvent 房间业务事件标准接口
// 所有广播、结算、玩家进出等操作都封装成事件
type RoomEvent interface {
	Execute(ctx context.Context, room IRoom)
}
