package handler

import (
	pb_push "github.com/k8s/muyi/api/pb/push"
	pb_room "github.com/k8s/muyi/api/pb/room"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/sender"
)

func NewRoomHandler() {
	h := &HandlerRoom{}
	common.RegisterRoomHandler(pb_room.CmdRoomKind_CMD_ROOM_CREATE, false, h.Create)
}

type HandlerRoom struct {
}

func (c *HandlerRoom) Create(ctx *common.TContext, req []byte, room common.IRoom) ([]byte, error) {
	clog := ctx.Logger
	clog.Debug("HandlerRoom Create demo")
	uId := ctx.Uid
	p := &pb_push.PushChat{Content: "HandlerRoom hello world"}
	//push.SinglePushUser(clog, uId, pb_push.CmdPushKind_Cmd_Chat, p)
	//room.SinglePush(clog, uId, pb_push.CmdPushKind_Cmd_Chat, p)
	one := sender.GetPushEvent(uId, []uint64{uId}, pb_push.CmdPushKind_Cmd_Chat, p)
	sender.PushEvent(one)
	return nil, nil
}
