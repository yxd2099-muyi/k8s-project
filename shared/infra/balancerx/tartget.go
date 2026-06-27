package balancerx

import (
	"sync"

	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

func RegisterTargetBalanceBuilder() {
	builder := &TargetBalancerBuilder{}
	balancer.Register(builder)
}

// Builder
type TargetBalancerBuilder struct{}

func (c *TargetBalancerBuilder) Name() string {
	return string(cconst.LBTargetDirect)
}

// BuildOptions：携带目标地址与自定义属性，本业务暂不使用，仅保留入参
func (c *TargetBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	clog := logger.L
	clog.Info("TargetBalancerBuilder build called--------", zap.Any("cc", cc), zap.Any("opts", opts))

	bl := &TargetBalancer{
		cc:         cc,
		opts:       opts,
		subConnMap: make(map[string]balancer.SubConn),
		readySet:   make(map[balancer.SubConn]bool),
		clog:       clog,
	}
	return bl
}

// Balancer主体
type TargetBalancer struct {
	mu         sync.RWMutex
	cc         balancer.ClientConn
	opts       balancer.BuildOptions       // 构建时的选项，保留备用
	subConnMap map[string]balancer.SubConn // key: ip:port
	readySet   map[balancer.SubConn]bool   // 标记subconn是否就绪
	clog       *zap.Logger
}

// 接收etcd resolver推送的地址列表
func (b *TargetBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	clog := b.clog
	clog.Info("UpdateClientConnState receive addrs", zap.Any("state", state))
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. 清理已经不在列表中的旧连接
	existAddr := make(map[string]bool)
	endPoints := state.ResolverState.Endpoints
	for _, point := range endPoints {
		address := point.Addresses
		for _, addr := range address {
			existAddr[addr.Addr] = true
		}
	}

	for addrStr, sc := range b.subConnMap {
		if !existAddr[addrStr] {
			sc.Shutdown()
			delete(b.subConnMap, addrStr)
			delete(b.readySet, sc)
		}
	}
	clog.Info("UpdateClientConnState end addrs", zap.Any("state", state), zap.Any("subConnMap", b.subConnMap))
	//遍历地址，不存在则创建SubConn
	for _, point := range endPoints {
		address := point.Addresses
		for _, addr := range address {
			addrStr := addr.Addr
			clog.Debug("UpdateClientConnState", zap.String("addr", addrStr))
			_, ok := b.subConnMap[addrStr]
			clog.Debug("UpdateClientConnState", zap.String("addr", addrStr), zap.Any("ok", ok))
			if ok {
				continue
			}

			sc, err := b.cc.NewSubConn(
				[]resolver.Address{addr},
				balancer.NewSubConnOptions{
					HealthCheckEnabled: false,
				},
			)
			if err != nil {
				logger.L.Error("create subconn failed",
					zap.String("addr", addrStr), zap.Error(err))
				continue
			}
			b.subConnMap[addrStr] = sc
			sc.Connect() // 主动发起TCP连接
		}
	}

	clog.Info("UpdateClientConnState end addrs", zap.Any("state", state), zap.Any("subConnMap", b.subConnMap))
	// 3. 刷新picker
	b.updatePickerLocked()
	return nil
}

// 监听每个SubConn的连接状态
func (b *TargetBalancer) UpdateSubConnState(sc balancer.SubConn, subState balancer.SubConnState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	clog := b.clog
	clog.Info("UpdateSubConnState receive addrs", zap.Any("sc", sc), zap.Any("subState", subState))
	switch subState.ConnectivityState {
	case connectivity.Ready:
		b.readySet[sc] = true
	case connectivity.TransientFailure, connectivity.Shutdown, connectivity.Idle:
		delete(b.readySet, sc)
	default:
		b.clog.Debug("update subconn state default")
	}

	b.updatePickerLocked()
}

// 构造picker，交给grpc做Pick路由
func (b *TargetBalancer) updatePickerLocked() {
	picker := &directPicker{
		parent: b,
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Ready,
		Picker:            picker,
	})
}

// ResolverError Resolver错误回调
func (b *TargetBalancer) ResolverError(err error) {
	logger.L.Error("resolver watch error", zap.Error(err))
}

// Close 关闭清理资源
func (b *TargetBalancer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sc := range b.subConnMap {
		sc.Shutdown()
	}
	b.subConnMap = nil
	b.readySet = nil
}

// ExitIdle 主动退出空闲，唤醒连接
func (b *TargetBalancer) ExitIdle() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sc := range b.subConnMap {
		sc.Connect()
	}
}

// Picker：根据metadata里的目标地址精准选择SubConn
type directPicker struct {
	parent *TargetBalancer
}

func (p *directPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	clog := p.parent.clog
	clog.Info("directPicker pick called", zap.Any("info", info))
	// 取出路由目标地址
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	if !ok {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	val := md.Get(cconst.ContextFieldRouterTargetAddress)
	if len(val) == 0 || val[0] == "" {
		clog.Warn("no target address in metadata")
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	targetAddr := val[0]
	clog.Info("directPicker targetAddr", zap.String("targetAddr", targetAddr))
	p.parent.mu.RLock()
	sc, ok := p.parent.subConnMap[targetAddr]
	ready := p.parent.readySet[sc]
	p.parent.mu.RUnlock()
	clog.Info("directPicker ready", zap.Bool("ready", ready), zap.Any("sc", sc))
	if !ok || !ready {
		clog.Warn("target subconn not found or not ready",
			zap.String("target", targetAddr))
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{SubConn: sc}, nil
}
