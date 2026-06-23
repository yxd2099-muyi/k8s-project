package room

import (
	"context"
	"sync"
	"time"

	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
)

const (
	roomMsgChanCap = 200 // 房间消息缓冲
	roomCloseWait  = 2 * time.Second
)

// RoomMsg 房间内部消息
type RoomMsg struct {
	tCtx    *common.TContext
	payload []byte
	rInfo   *common.RouterInfo
}

type Room struct {
	roomId    uint32
	msgChan   chan *RoomMsg
	closeChan chan struct{}
	wg        sync.WaitGroup
	players   map[uint64]*Player // 仅room协程读写，无锁
	log       *zap.Logger
}

func NewRoom(roomId uint32) *Room {
	r := &Room{
		roomId:    roomId,
		msgChan:   make(chan *RoomMsg, roomMsgChanCap),
		closeChan: make(chan struct{}),
		players:   make(map[uint64]*Player),
		log:       logger.L.With(zap.Uint32("roomId", roomId)),
	}
	r.wg.Add(1)
	go r.run()
	return r
}

// run 房间专属worker协程，串行处理所有消息，天然线程安全
func (r *Room) run() {
	defer r.wg.Done()
	r.log.Info("room worker start")
	for {
		select {
		case <-r.closeChan:
			r.log.Info("room worker exit")
			return
		case msg := <-r.msgChan:
			r.handleMsg(msg)
		}
	}
}

func (r *Room) handleMsg(msg *RoomMsg) {
	defer func() {
		if err := recover(); err != nil {
			r.log.Error("room handle panic", zap.Any("err", err))
		}
	}()
	info := msg.rInfo
	if info.IsSync {
		_, _ = info.Handler(msg.tCtx, msg.payload)
		return
	}
	// 异步指令，不阻塞grpc响应
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.log.Error("async room cmd panic", zap.Any("err", err))
			}
		}()
		_, _ = info.Handler(msg.tCtx, msg.payload)
	}()
}

// SendMsg 投递消息到房间队列，非阻塞
func (r *Room) SendMsg(tCtx *common.TContext, payload []byte, info *common.RouterInfo) bool {
	rm := &RoomMsg{
		tCtx:    tCtx,
		payload: payload,
		rInfo:   info,
	}
	select {
	case r.msgChan <- rm:
		return true
	default:
		r.log.Warn("room msg chan full drop msg")
		return false
	}
}

// AddPlayer 加入房间
func (r *Room) AddPlayer(uid uint64) {
	if _, ok := r.players[uid]; !ok {
		r.players[uid] = NewPlayer(uid)
	}
}

// DelPlayer 离开房间
func (r *Room) DelPlayer(uid uint64) {
	delete(r.players, uid)
}

// Broadcast 全房间广播
func (r *Room) Broadcast(payload []byte) {
	uids := make([]uint64, 0, len(r.players))
	for uid := range r.players {
		uids = append(uids, uid)
	}
	//todo
	//r.pushMgr.Push(&common.PushMsg{
	//	UidList: uids,
	//	Payload: payload,
	//})
}

// Close 优雅关闭房间，等待worker退出
func (r *Room) Close() {
	close(r.closeChan)
	// 等待处理完现有消息
	ctx, cancel := context.WithTimeout(context.Background(), roomCloseWait)
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		r.log.Warn("room close wait timeout")
	}
}

func (r *Room) RoomID() uint32 {
	return r.roomId
}
