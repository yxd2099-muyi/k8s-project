package handler

import (
	pb_room "github.com/k8s/muyi/api/pb/room"
	"github.com/k8s/muyi/internal/game/common"
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
	return nil, nil
}
