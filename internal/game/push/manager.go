package push

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/api/db"
	pb_push "github.com/k8s/muyi/api/pb/push"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"

	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ===================== 全局配置常量，可放config统一读取 =====================
const (
	// 推送RPC超时
	pushRPCTimeout = 3 * time.Second
	// 批量推送最大并发goroutine，防止瞬间爆协程
	batchPushMaxWorker = 20
)

// gateClientWrap 封装连接+客户端，方便关闭、重连
type gateClientWrap struct {
	conn *grpc.ClientConn
	cli  pb_service.GatePushClient
}

// PushManager 推送管理器
type PushManager struct {
	rwMu     sync.RWMutex // 读写锁，读多写少提升性能
	gateConn map[string]*gateClientWrap

	workerPool chan struct{} // 协程池限流
	clog       *zap.Logger
	userDb     *db.User
}

// ===================== 全局单例 =====================
var (
	GlobalMgr *PushManager
	once      sync.Once
)

// InitGlobalPushMgr 服务启动时调用一次初始化全局推送管理器
func InitGlobalPushMgr() *PushManager {
	once.Do(func() {
		GlobalMgr = NewPushManager()
	})
	return GlobalMgr
}

// NewPushManager 构造
func NewPushManager() *PushManager {
	return &PushManager{
		gateConn:   make(map[string]*gateClientWrap),
		workerPool: make(chan struct{}, batchPushMaxWorker),
		clog:       logger.L,
		userDb:     db.NewUserObj(),
	}
}

// getUserGateAddr 获取用户所在网关地址（业务自行实现redis查询）
func (p *PushManager) getUserGateAddr(clog *zap.Logger, uid uint64) (string, error) {
	// 示例：从redis/内存路由表查询uid绑定gate地址
	ctx, cancel := context.WithTimeout(context.Background(), cconst.ContextTimeOut3s)
	defer cancel()
	s, err := p.userDb.GetUserSession(ctx, clog, uid)
	if err != nil {
		return "", err
	}
	addr := s.GateAddress
	//addr = "172.16.111.60:8099"
	return addr, nil
}

func (p *PushManager) userGateGroup(clog *zap.Logger, uids []uint64) (map[string][]uint64, error) {
	// 示例：从redis/内存路由表查询uid绑定gate地址
	ctx, cancel := context.WithTimeout(context.Background(), cconst.ContextTimeOut3s)
	defer cancel()
	s, err := p.userDb.GetSomeUsersSession(ctx, clog, uids)
	if err != nil {
		return nil, err
	}
	clog.Debug("get gate addr", zap.Uint64s("uids", uids), zap.Any("session", s), zap.Any("size", len(s)))
	gateGroup := make(map[string][]uint64)
	for _, session := range s {
		addr, uid := session.GateAddress, session.UserID
		//addr, uid := "172.16.111.60:8099", session.UserID
		gateGroup[addr] = append(gateGroup[addr], uid)
	}
	clog.Debug("gate group", zap.Any("gateGroup", gateGroup))
	return gateGroup, nil
}

// getGateCtx 生成标准推送ctx：带超时、可扩展metadata透传trace
func getGatePushCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), pushRPCTimeout)
	// 如需链路追踪、服务标识可追加metadata
	// ctx = metadata.AppendToOutgoingContext(ctx, "service", "game-server")
	return ctx, cancel
}

// getGateClient 读写锁优化获取gate客户端，失败自动清理连接
func (p *PushManager) getGateClient(gateAddr string) (pb_service.GatePushClient, error) {
	// 读锁：高频查询无竞争
	p.rwMu.RLock()
	wrap, ok := p.gateConn[gateAddr]
	p.rwMu.RUnlock()
	if ok {
		return wrap.cli, nil
	}

	// 不存在，写锁新建连接
	p.rwMu.Lock()
	defer p.rwMu.Unlock()
	// 双重检测，防止并发重复创建
	if wrap, ok = p.gateConn[gateAddr]; ok {
		return wrap.cli, nil
	}

	// 拨号创建连接
	conn, err := grpc.NewClient(gateAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.clog.Error("dial gate server failed", zap.String("gate_addr", gateAddr), zap.Error(err))
		return nil, fmt.Errorf("dial gate %s err: %w", gateAddr, err)
	}

	wrap = &gateClientWrap{
		conn: conn,
		cli:  pb_service.NewGatePushClient(conn),
	}
	p.gateConn[gateAddr] = wrap
	p.clog.Info("create new gate grpc conn success", zap.String("gate_addr", gateAddr))
	return wrap.cli, nil
}

// delGateClient 网关故障时删除失效连接并关闭底层conn
func (p *PushManager) delGateClient(gateAddr string) {
	p.rwMu.Lock()
	defer p.rwMu.Unlock()
	wrap, ok := p.gateConn[gateAddr]
	if !ok {
		return
	}
	// 关闭底层grpc连接
	_ = wrap.conn.Close()
	delete(p.gateConn, gateAddr)
	p.clog.Warn("remove broken gate conn", zap.String("gate_addr", gateAddr))
}

// BatchPushUser 批量推送uid列表（带协程限流、超时ctx、故障清理）
func (p *PushManager) BatchPushUser(clog *zap.Logger, uids []uint64, pushBody *pb_base.PushBody) {
	if len(uids) == 0 || pushBody == nil {
		return
	}
	p.clog.Debug("batch push user", zap.Int("uids", len(uids)))
	// 1. 按网关地址分组uid
	gateGroup, err := p.userGateGroup(clog, uids)
	if err != nil {
		p.clog.Error("get gate group failed", zap.Error(err))
		return
	}
	if len(gateGroup) == 0 {
		return
	}
	p.clog.Info("get gate group success", zap.Any("gate_group", gateGroup))
	var wg sync.WaitGroup
	// 2. 每个网关一条推送协程，受workerPool限流
	for gateAddr, targetUids := range gateGroup {
		wg.Add(1)
		p.workerPool <- struct{}{} // 占坑限流

		go func(addr string, uids []uint64) {
			defer func() {
				<-p.workerPool // 释放坑位
				wg.Done()
			}()

			cli, err := p.getGateClient(addr)
			if err != nil {
				return
			}

			// 强制使用带超时ctx，杜绝background裸上下文
			ctx, cancel := getGatePushCtx()
			defer cancel()

			req := &pb_service.PushReq{
				Body: pushBody,
				Uids: uids,
			}
			_, err = cli.Push(ctx, req)
			if err != nil {
				clog.Error("gate push failed",
					zap.Any("gate_addr", addr),
					zap.Any("uid_count", len(uids)),
					zap.Error(err),
				)
				// 网关推送失败，删除失效连接，下次自动重连
				p.delGateClient(addr)
				return
			}
			clog.Debug("gate batch push success", zap.Any("gate_addr", addr), zap.Any("uid_num", len(uids)))
		}(gateAddr, targetUids)
	}
	wg.Wait()
}

// SinglePushUser 单用户推送封装（业务高频使用）
func (p *PushManager) SinglePushUser(clog *zap.Logger, uid uint64, pushBody *pb_base.PushBody) {
	p.BatchPushUser(clog, []uint64{uid}, pushBody)
	return
}

// Shutdown 服务关闭时释放所有grpc连接
func (p *PushManager) Shutdown() {
	p.rwMu.Lock()
	defer p.rwMu.Unlock()

	for addr, wrap := range p.gateConn {
		_ = wrap.conn.Close()
		p.clog.Info("close gate grpc conn", zap.Any("gate_addr", addr))
	}
	p.gateConn = nil
	close(p.workerPool)
	p.clog.Info("push manager shutdown complete")
}

// SinglePushUser 给单个人放消息
func SinglePushUser(clog *zap.Logger, uid uint64, cmd pb_push.CmdPushKind, p proto.Message) {
	payload, _ := serializer.EncodeProto(p)
	req := &pb_base.PushBody{}
	req.Cmd = uint32(cmd)
	req.Payload = payload
	GlobalMgr.SinglePushUser(clog, uid, req)
	return
}

// BatchPushUser 给多人发送
func BatchPushUser(clog *zap.Logger, uids []uint64, cmd pb_push.CmdPushKind, p proto.Message) {
	payload, _ := serializer.EncodeProto(p)
	req := &pb_base.PushBody{}
	req.Cmd = uint32(cmd)
	req.Payload = payload
	GlobalMgr.BatchPushUser(clog, uids, req)
	return
}
