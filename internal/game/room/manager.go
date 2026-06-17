package room

import (
	pb_base "github.com/k8s/muyi/api/pb/base"
	"sync"
	"sync/atomic"
)

type Room struct {
	roomId uint64
	uids   map[uint64]bool
	mu     sync.RWMutex
}

type RoomMgr struct {
	mu       sync.RWMutex
	rooms    map[uint64]*Room
	maxNum   int
	count    atomic.Int32
	shutdown atomic.Bool
}

func NewRoomMgr(maxRoom int) *RoomMgr {
	return &RoomMgr{
		rooms:  make(map[uint64]*Room),
		maxNum: maxRoom,
	}
}

// CreateRoom 创建房间
func (m *RoomMgr) CreateRoom(roomId uint64) pb_base.ErrCode {
	if m.shutdown.Load() {
		return pb_base.ErrCode_EC_INTERNAL_ERR
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if int(m.count.Load()) >= m.maxNum {
		return pb_base.ErrCode_EC_ROOM_FULL
	}
	if _, ok := m.rooms[roomId]; ok {
		return pb_base.ErrCode_EC_OK
	}
	m.rooms[roomId] = &Room{
		roomId: roomId,
		uids:   make(map[uint64]bool),
	}
	m.count.Add(1)
	return pb_base.ErrCode_EC_OK
}

// GetRoom 获取房间
func (m *RoomMgr) GetRoom(roomId uint64) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rooms[roomId]
	return r, ok
}

// Shutdown 清理房间
func (m *RoomMgr) Shutdown() {
	if m.shutdown.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rooms = nil
}
