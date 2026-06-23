package room

import (
	"sync/atomic"
)

type Player struct {
	uid    uint64
	online int32 // 1在线 0离线
	// 玩家业务状态，自行扩展
	hp   int32
	posX float32
	posY float32
}

func NewPlayer(uid uint64) *Player {
	return &Player{
		uid:    uid,
		online: 1,
	}
}

func (p *Player) UID() uint64 {
	return p.uid
}

func (p *Player) IsOnline() bool {
	return atomic.LoadInt32(&p.online) == 1
}

func (p *Player) Offline() {
	atomic.StoreInt32(&p.online, 0)
}
