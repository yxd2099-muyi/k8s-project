package grpclib

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
	"time"
)

// PoolOption 连接池配置选项
type PoolOption func(cfg *poolConfig)

type poolConfig struct {
	dialOpts []grpc.DialOption
	dialCtx  context.Context
}

// Pool gRPC连接池（字段全部私有，禁止外部篡改）
type Pool struct {
	target string
	size   int
	conns  []*grpc.ClientConn

	mu     sync.RWMutex // RWMutex 读多写少优化，GetConn读共享锁、Close写排他锁
	idx    uint64       // 轮询索引，原子操作
	closed atomic.Bool  // 池关闭标记

	// 监控埋点
	getConnTotal atomic.Uint64 // 累计获取连接次数
	badConnCount atomic.Uint64 // 失效连接计数
}

// WithDialOpts 自定义grpc dial配置
func WithDialOpts(opts ...grpc.DialOption) PoolOption {
	return func(cfg *poolConfig) {
		cfg.dialOpts = append(cfg.dialOpts, opts...)
	}
}

// WithDialContext 创建连接时使用指定上下文控制超时
func WithDialContext(ctx context.Context) PoolOption {
	return func(cfg *poolConfig) {
		cfg.dialCtx = ctx
	}
}

// NewPool 创建连接池
// target: grpc服务地址
// size: 连接池固定连接数量，必须>0
// opts: 自定义dial参数
func NewPool(target string, size int, opts ...PoolOption) (*Pool, error) {
	// 参数合法性校验
	if target == "" {
		return nil, errors.New("target cannot be empty")
	}
	if size <= 0 {
		return nil, errors.New("pool size must greater than 0")
	}

	// 加载自定义配置
	cfg := &poolConfig{
		dialCtx: context.Background(),
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}

	p := &Pool{
		target: target,
		size:   size,
		conns:  make([]*grpc.ClientConn, 0, size),
	}

	// 批量创建连接，中途失败自动关闭已创建连接防止泄漏
	for i := 0; i < size; i++ {
		conn, err := grpc.NewClient(target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: 5 * time.Second, // 连接超时
				Backoff: backoff.Config{
					BaseDelay:  1.0 * time.Second,
					Multiplier: 1.6,
					Jitter:     0.2,
				},
			}),
		)
		if err != nil {
			for _, c := range p.conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("create conn %d failed: %w", i, err)
		}
		p.conns = append(p.conns, conn)
	}

	return p, nil
}

// GetConn 无锁轮询获取连接
// 池已关闭返回nil，上层需要判空
func (p *Pool) GetConn() *grpc.ClientConn {
	return p.GetConnCtx(context.Background())
}

// GetConnCtx 带上下文获取连接（预留扩展，可做限流、超时控制）
func (p *Pool) GetConnCtx(_ context.Context) *grpc.ClientConn {
	// 池已关闭直接返回nil
	if p.closed.Load() {
		return nil
	}
	p.getConnTotal.Add(1)

	// RLock 读共享锁，高并发无争抢，仅Close写时阻塞
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 双重校验：Close过程中可能置空conns
	if len(p.conns) == 0 {
		return nil
	}

	// 原子自增实现无锁轮询
	idx := atomic.AddUint64(&p.idx, 1) % uint64(len(p.conns))
	return p.conns[idx]
}

// Close 关闭所有连接，幂等，重复调用无副作用
func (p *Pool) Close() {
	// 快速判断是否已关闭
	if p.closed.Swap(true) {
		return
	}

	// 写排他锁，阻塞所有GetConn读操作
	p.mu.Lock()
	defer p.mu.Unlock()

	// 遍历关闭所有连接
	for _, c := range p.conns {
		_ = c.Close()
	}
	// 置空切片，防止并发GetConn读取失效数组
	p.conns = nil
}

// IsClosed 查询池是否已关闭
func (p *Pool) IsClosed() bool {
	return p.closed.Load()
}

// Len 返回当前有效连接数量
func (p *Pool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

// GetMetrics 获取池监控指标
func (p *Pool) GetMetrics() (totalGet uint64, badConn uint64) {
	return p.getConnTotal.Load(), p.badConnCount.Load()
}

// HealthCheck 基础连接健康检测（简易版）
// 原理：grpc ClientConn 内部会维护连接状态，可通过GetState判断
func (p *Pool) HealthCheck() int {
	if p.closed.Load() {
		return 0
	}
	broken := 0
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, c := range p.conns {
		state := c.GetState()
		// 新版grpc状态常量
		switch state {
		case connectivity.Idle, connectivity.TransientFailure, connectivity.Shutdown:
			broken++
		default:
		}
	}
	p.badConnCount.Store(uint64(broken))
	return broken
}
