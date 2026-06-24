package room

import (
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
)

type RoomMgr struct {
	rooms    sync.Map // key:uint32 roomId, value:*Room
	log      *zap.Logger
	closeSig chan struct{}
	wg       sync.WaitGroup
}

func NewRoomMgr(pushWorkerNum int) *RoomMgr {
	mgr := &RoomMgr{
		log:      logger.L.Named("room_mgr"),
		closeSig: make(chan struct{}),
	}
	return mgr
}

// GetOrCreateRoom 获取房间，不存在则新建
func (m *RoomMgr) GetOrCreateRoom(roomId uint32) common.IRoom {
	val, ok := m.rooms.Load(roomId)
	if ok {
		return val.(*Room)
	}
	// 新建房间
	newRoom := NewRoom(roomId)
	m.rooms.Store(roomId, newRoom)
	m.log.Info("create room success", zap.Uint32("roomId", roomId))
	return newRoom
}

// DelRoom 销毁房间
func (m *RoomMgr) DelRoom(roomId uint32) {
	val, ok := m.rooms.LoadAndDelete(roomId)
	if !ok {
		return
	}
	room := val.(*Room)
	room.Close()
	m.log.Info("destroy room success", zap.Uint32("roomId", roomId))
}

// GetRoom 仅查询，不创建
func (m *RoomMgr) GetRoom(roomId uint32) (common.IRoom, bool) {
	val, ok := m.rooms.Load(roomId)
	if !ok {
		return nil, false
	}
	return val.(*Room), true
}

// Close 全局优雅关闭所有房间、推送协程
func (m *RoomMgr) Close() {
	close(m.closeSig)
	// 关闭推送
	// 遍历销毁所有房间
	m.rooms.Range(func(k, v any) bool {
		room := v.(*Room)
		room.Close()
		m.rooms.Delete(k)
		return true
	})
	m.log.Info("all room closed")
}
