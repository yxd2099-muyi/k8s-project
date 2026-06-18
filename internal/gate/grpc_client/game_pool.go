package grpc_client

import (
	"fmt"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/grpclib"

	"sync"
)

type GamePoolMgr struct {
	mu    sync.RWMutex
	pools map[string]*grpclib.Pool // key: game pod地址
	size  int
}

func NewGamePoolMgr(poolSize int) *GamePoolMgr {
	return &GamePoolMgr{
		pools: make(map[string]*grpclib.Pool),
		size:  poolSize,
	}
}

// GetClient 根据gameAddr获取GameLogic grpc客户端
func (m *GamePoolMgr) GetClient(gameAddr string) (pb_service.GameLogicClient, error) {
	m.mu.RLock()
	p, ok := m.pools[gameAddr]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		p, ok = m.pools[gameAddr]
		if !ok {
			newPool, err := grpclib.NewPool(gameAddr, m.size)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			m.pools[gameAddr] = newPool
			p = newPool
		}
		m.mu.Unlock()
	}
	conn := p.GetConn()
	if conn == nil {
		return nil, fmt.Errorf("target %s grpc pool unavailable", gameAddr)
	}
	return pb_service.NewGameLogicClient(conn), nil
}

// Shutdown 关闭所有game grpc连接池
func (m *GamePoolMgr) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		p.Close()
	}
	m.pools = nil
}
