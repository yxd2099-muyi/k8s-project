package grpclib

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
)

type Pool struct {
	target string
	size   int
	conns  []*grpc.ClientConn
	mu     sync.Mutex
	idx    uint64
	closed atomic.Bool
}

func NewPool(target string, size int) (*Pool, error) {
	p := &Pool{
		target: target,
		size:   size,
		conns:  make([]*grpc.ClientConn, 0, size),
	}
	for i := 0; i < size; i++ {
		//conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials())) // 这里需要替换为当前推荐的方式。 要看下正常是如何使用的
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		p.conns = append(p.conns, conn)
	}
	return p, nil
}

// GetConn 轮询获取连接
func (p *Pool) GetConn() *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := atomic.AddUint64(&p.idx, 1) % uint64(p.size)
	return p.conns[idx]
}

// Close 关闭全部连接
func (p *Pool) Close() {
	if p.closed.Swap(true) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}
