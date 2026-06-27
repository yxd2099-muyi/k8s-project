package grpcx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k8s/muyi/shared/infra/balancerx"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientConfig gRPC客户端配置
type ClientConfig struct {
	Target           string        // 完整地址：etcd:///service-prefix
	LBPolicy         string        // 负载均衡策略: round_robin / consistent_hash
	Timeout          time.Duration // 建立连接超时上下文
	KeepaliveTime    time.Duration // TCP保活心跳间隔
	KeepaliveTimeout time.Duration // 心跳超时判定断开
	WaitForReady     bool          // RPC调用是否等待节点就绪，不快速失败
}

// DefaultClientConfig 默认配置
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Target:           "etcd:///my-service",
		LBPolicy:         "round_robin",
		Timeout:          10 * time.Second,
		KeepaliveTime:    120 * time.Second,
		KeepaliveTimeout: 20 * time.Second,
		WaitForReady:     true,
	}
}

// serviceConfig 用于JSON序列化，避免字符串拼接出错
type serviceConfig struct {
	LoadBalancingConfig []map[string]json.RawMessage `json:"loadBalancingConfig"`
	WaitForReady        bool                         `json:"waitForReady"`
	// 禁止降级到pick_first
	FallbackPolicy json.RawMessage `json:"fallbackPolicy,omitempty"`
}

// GrpcClient gRPC连接封装
// 说明：
// 1. *grpc.ClientConn 本身并发安全，多goroutine共用
// 2. closed标记仅做关闭幂等控制
type GrpcClient struct {
	conn   *grpc.ClientConn
	mu     sync.Mutex
	clog   *zap.Logger
	closed bool
}

// NewGrpcClient 创建gRPC客户端
// etcdClient 由外部调用方管理生命周期，本方法不会关闭etcdClient
func NewGrpcClient(cfg ClientConfig, etcdClient *clientv3.Client) (*GrpcClient, error) {
	balancerx.RegisterTargetBalanceBuilder()
	if etcdClient == nil {
		return nil, errors.New("etcd client must not be nil")
	}
	clog := logger.L
	// 强制校验target必须携带etcd scheme
	const schemePrefix = "etcd:///"
	if len(cfg.Target) < len(schemePrefix) || cfg.Target[:len(schemePrefix)] != schemePrefix {
		return nil, fmt.Errorf("target must start with %s, got: %s", schemePrefix, cfg.Target)
	}

	// 1. 构建负载均衡服务配置（使用JSON序列化，杜绝语法错误）
	svcCfg := serviceConfig{
		WaitForReady: cfg.WaitForReady,
		LoadBalancingConfig: []map[string]json.RawMessage{
			{
				cfg.LBPolicy: json.RawMessage("{}"),
			},
		},
		FallbackPolicy: json.RawMessage(`{"fallbackBackoffMultiplier":1}`),
	}
	svcConfigBytes, err := json.Marshal(svcCfg)
	if err != nil {
		return nil, fmt.Errorf("marshal service config: %w", err)
	}
	svcConfigJSON := string(svcConfigBytes)
	clog.Info("client config", zap.Any("cfg", cfg))
	clog.Info("service config", zap.String("service_config", svcConfigJSON))

	etcdResolverBuilder, err := resolver.NewBuilder(etcdClient)
	if err != nil {
		return nil, fmt.Errorf("create etcd resolver builder: %w", err)
	}

	connParams := grpc.ConnectParams{
		MinConnectTimeout: 5 * time.Second, // 最小连接超时，确保 SubConn 主动连接
	}
	// 3. Dial参数
	dialOpts := []grpc.DialOption{
		// 注入etcd服务发现解析器
		grpc.WithResolvers(etcdResolverBuilder),

		// 全局服务配置：LB策略 + waitForReady
		grpc.WithDefaultServiceConfig(svcConfigJSON),

		// 内网非加密通信；公网生产替换为TLS证书凭证
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(connParams),
		// TCP保活，防止空闲长连接被防火墙静默断开
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepaliveTime,
			Timeout:             cfg.KeepaliveTimeout,
			PermitWithoutStream: true, // 无业务流也持续发送心跳包
		}),
		// ==========关键新增这一行==========
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}

	conn, err := grpc.NewClient(cfg.Target, dialOpts...)
	if err != nil {
		clog.Error("new grpc client", zap.Error(err))
		return nil, fmt.Errorf("grpc.NewClient failed: %w", err)
	}
	// 强制激活连接，跳出IDLE，立刻执行服务发现+TCP握手
	conn.Connect()
	gc := &GrpcClient{
		conn: conn,
		clog: logger.L,
	}
	clog.Info("grpc client  eeeeeee")
	go gc.watchConnState()
	return gc, nil
}

// watchConnState 监听连接状态变化，输出日志
func (c *GrpcClient) watchConnState() {
	clog := c.clog
	conn := c.conn
	defer func() {
		if err := recover(); err != nil {
			clog.Error("watchConnState", zap.Any("connState", conn), zap.Any("err", err))
		}
	}()
	currentState := connectivity.Idle
	for {
		// 等待状态变更
		conn.WaitForStateChange(context.Background(), currentState)
		newState := conn.GetState()
		clog.Info("watchConnState newState", zap.Any("state", newState))
		if newState == currentState {
			continue
		}
		clog.Info(fmt.Sprintf("[grpc-conn] state changed: %s → %s", currentState, newState))
		currentState = newState
		clog.Info("watchConnState currentState", zap.Any("state", newState))
		// 连接彻底关闭，退出监控协程
		if newState == connectivity.Shutdown {
			clog.Error(fmt.Sprintf("[grpc-conn] connection shutdown, exit state watcher"))
			return
		}
	}
}

// Conn 获取原生ClientConn，用于生成RPC Stub
// 只读成员，无需加锁，并发安全
func (c *GrpcClient) Conn() *grpc.ClientConn {
	return c.conn
}

// Close 优雅关闭连接，保证幂等，重复调用不会报错
func (c *GrpcClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("close grpc conn: %w", err)
	}
	c.clog.Info("[grpc-conn] client connection closed gracefully")
	return nil
}

// IsClosed 查询关闭状态
func (c *GrpcClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
