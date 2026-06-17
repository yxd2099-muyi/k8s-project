package handler

import (
	pb_push "github.com/k8s/muyi/api/pb/push"
	pb_room "github.com/k8s/muyi/api/pb/room"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/push"
)

func NewHandler() {
	h := &Handler{}
	common.RegisterRoomHandler(pb_room.CmdRoomKind_CMD_ROOM_CREATE, false, h.Create)
}

type Handler struct {
}

func (c *Handler) Create(ctx *common.TContext, req []byte) ([]byte, error) {
	clog := ctx.Logger
	clog.Debug("Create demo")
	uId := ctx.Uid
	p := &pb_push.PushChat{Content: "hello world"}
	push.SinglePushUser(uId, pb_push.CmdPushKind_Cmd_Chat, p)
	return nil, nil
}
