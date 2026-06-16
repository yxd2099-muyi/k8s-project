package hub

import (
	"context"
	"github.com/k8s/muyi/internal/gate/client_conn"
	"sync"
	"sync/atomic"
)

type Hub struct {
	ctx    context.Context
	cancel context.CancelFunc
	// uid -> *client_conn.ClientConn
	conns sync.Map
	close atomic.Bool
	wg    sync.WaitGroup
}

func NewHub() *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		ctx:    ctx,
		cancel: cancel,
	}
}

// AddConn 注册客户端连接
func (h *Hub) AddConn(uid uint64, cli *client_conn.ClientConn) bool {
	if h.close.Load() {
		return false
	}
	h.conns.Store(uid, cli)
	h.wg.Add(1)
	return true
}

// DelConn 删除连接
func (h *Hub) DelConn(uid uint64) {
	h.conns.Delete(uid)
	h.wg.Done()
}

// GetConn 获取单个用户连接
func (h *Hub) GetConn(uid uint64) (*client_conn.ClientConn, bool) {
	val, ok := h.conns.Load(uid)
	if !ok {
		return nil, false
	}
	return val.(*client_conn.ClientConn), true
}

// BatchPush 批量uid推送消息
func (h *Hub) BatchPush(uids []uint64, data []byte) {
	for _, uid := range uids {
		cli, ok := h.GetConn(uid)
		if !ok {
			continue
		}
		_ = cli.WriteMsg(data)
	}
}

// Shutdown 关闭hub，等待所有连接退出
func (h *Hub) Shutdown() {
	if h.close.Swap(true) {
		return
	}
	h.cancel()
	h.conns.Range(func(k, v any) bool {
		cli := v.(*client_conn.ClientConn)
		cli.Close()
		return true
	})
	h.wg.Wait()
}
