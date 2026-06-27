package balancerx

import (
	"fmt"
	"sync"

	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
)

func init() {
	balancer.Register(NewTargetDirectBuilder())
	fmt.Println("==============NewTargetDirectBuilder==========================")
}

//func InitTargetDirectBalanceBuilder() {
//  logger.L.Error("InitTargetDirectBalanceBuilder")
//  balancer.Register(NewTargetDirectBuilder())
//}

type targetDirectPickerBuilder struct {
}

func NewTargetDirectBuilder() balancer.Builder {
	return base.NewBalancerBuilder(
		//"target_direct",
		string(cconst.LBTargetDirect),
		&targetDirectPickerBuilder{},
		base.Config{HealthCheck: false},
		//base.Config{HealthCheck: false},
	)
}

// PickerBuildInfo 会多次调用，需支持更新
func (b *targetDirectPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	clog := logger.L
	clog.Info("【TargetDirect】Build called--------",
		zap.Int("readySCs", len(info.ReadySCs)))

	picker := &targetDirectPicker{
		addrMap: make(map[string]balancer.SubConn),
	}

	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		picker.addrMap[addr] = sc
		clog.Info("【TargetDirect】Registered", zap.String("addr", addr))
	}

	return picker
}

type targetDirectPicker struct {
	mu      sync.RWMutex
	addrMap map[string]balancer.SubConn
}

func (p *targetDirectPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	clog := logger.L

	// 从 metadata 获取目标地址
	md, ok := metadata.FromOutgoingContext(info.Ctx)
	if !ok {
		clog.Warn("【TargetDirect】No metadata")
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	values := md.Get(cconst.ContextFieldRouterTargetAddress)
	if len(values) == 0 || values[0] == "" {
		clog.Warn("【TargetDirect】No target address in metadata")
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	targetAddr := values[0]

	p.mu.RLock()
	sc, exists := p.addrMap[targetAddr]
	p.mu.RUnlock()

	if !exists {
		clog.Warn("【TargetDirect】Address not found",
			zap.String("target", targetAddr),
			zap.Any("available", p.getAddrs()))
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{
		SubConn: sc,
	}, nil
}

func (p *targetDirectPicker) getAddrs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	addrs := make([]string, 0, len(p.addrMap))
	for a := range p.addrMap {
		addrs = append(addrs, a)
	}
	return addrs
}
