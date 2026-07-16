package k8s

import (
	"github.com/k8s/muyi/api/model"
	"github.com/k8s/muyi/internal/game/common"
)

// RoomRangeCalc 计算当前pod房间区间
type RoomRangeCalc struct {
	maxRoomCfg uint32
	podIndex   uint32
	RoomMin    uint32
	RoomMax    uint32
	PodName    string
	PodIP      string
	GrpcAddr   string
}

func NewRoomRangeCalc(maxRoomCfg uint32) (*RoomRangeCalc, error) {
	r := &RoomRangeCalc{}
	r.maxRoomCfg = maxRoomCfg
	if maxRoomCfg == 0 {
		r.maxRoomCfg = 200
	}
	argCfg := common.GetArg()
	r.PodIP = argCfg.IPString
	r.PodName = argCfg.PodName
	r.GrpcAddr = argCfg.RegisterAddr
	idx := argCfg.PodIndex
	r.podIndex = idx
	r.RoomMin = idx*r.maxRoomCfg + 1
	r.RoomMax = (idx + 1) * r.maxRoomCfg
	return r, nil
}

// IsRoomBelong 判断roomId是否属于当前pod
func (r *RoomRangeCalc) IsRoomBelong(roomId uint32) bool {
	return roomId >= r.RoomMin && roomId <= r.RoomMax
}

// GetGameNode 输出注册etcd的节点信息
func (r *RoomRangeCalc) GetGameNode() model.GameNode {
	return model.GameNode{
		PodName:    r.PodName,
		PodIP:      r.PodIP,
		GrpcAddr:   r.GrpcAddr,
		RoomMin:    r.RoomMin,
		RoomMax:    r.RoomMax,
		MaxRoomCfg: r.maxRoomCfg,
	}
}
