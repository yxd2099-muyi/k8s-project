package push

import (
	"context"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

type PushManager struct {
	mu       sync.Mutex
	gateConn map[string]pb_service.GatePushClient
}

func NewPushManager() *PushManager {
	return &PushManager{
		gateConn: make(map[string]pb_service.GatePushClient),
	}
}

// getUserGateAddr 获取用户gateserver的地址
func (p *PushManager) getUserGateAddr(uid uint64) (string, error) {
	return "", nil
}

// BatchPushUser 根据uid列表查询gate，分gate批量推送
func (p *PushManager) BatchPushUser(uids []uint64, pushBody *pb_base.PushBody) {
	// 按gate地址分组uid
	gateGroup := make(map[string][]uint64)
	for _, uid := range uids {
		gateAddr, err := p.getUserGateAddr(uid)
		if err != nil || gateAddr == "" {
			continue
		}
		gateGroup[gateAddr] = append(gateGroup[gateAddr], uid)
	}
	// 每个gate并发推送
	var wg sync.WaitGroup
	for gateAddr, targetUids := range gateGroup {
		wg.Add(1)
		go func(addr string, uids []uint64) {
			defer wg.Done()
			cli, err := p.getGateClient(addr)
			if err != nil {
				return
			}
			_, _ = cli.Push(context.Background(), &pb_service.PushReq{
				Body: pushBody,
				Uids: uids,
			})
		}(gateAddr, targetUids)
	}
	wg.Wait()
}

func (p *PushManager) getGateClient(gateAddr string) (pb_service.GatePushClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cli, ok := p.gateConn[gateAddr]; ok {
		return cli, nil
	}
	conn, err := grpc.Dial(gateAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	cli := pb_service.NewGatePushClient(conn)
	p.gateConn[gateAddr] = cli
	return cli, nil
}

func (p *PushManager) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gateConn = nil
}
