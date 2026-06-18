package conn

import (
	"sync"
)

type ConnManager struct {
	conns sync.Map //uid -> *client_conn.ClientConn
}

func NewConnMgr() *ConnManager {
	return &ConnManager{}
}

// AddConn 注册客户端连接
func (m *ConnManager) AddConn(uid uint64, cli *ClientConn) bool {
	m.conns.Store(uid, cli)
	return true
}

// DelConn 删除连接
func (m *ConnManager) DelConn(uid uint64) {
	client, ok := m.conns.Load(uid)
	if !ok {
		return
	}
	cli, ok := client.(*ClientConn)
	if ok {
		cli.Close()
	}
	m.conns.Delete(uid)
}

// GetConn 获取单个用户连接
func (m *ConnManager) GetConn(uid uint64) (*ClientConn, bool) {
	val, ok := m.conns.Load(uid)
	if !ok {
		return nil, false
	}
	return val.(*ClientConn), true
}

// BatchPush 批量uid推送消息
func (m *ConnManager) BatchPush(uids []uint64, data []byte) {
	for _, uid := range uids {
		cli, ok := m.GetConn(uid)
		if !ok {
			continue
		}
		_ = cli.WriteMsg(data)
	}
}

// Shutdown 关闭，等待所有连接退出
func (m *ConnManager) Shutdown() {
	m.conns.Range(func(k, v any) bool {
		cli, ok := v.(*ClientConn)
		if ok {
			cli.Close()
		}
		m.conns.Delete(k)
		return true
	})
}
