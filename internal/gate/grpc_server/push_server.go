package grpc_server

import (
	"context"
	"github.com/k8s/muyi/api/etcdapi"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/common"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/grpcx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
	"sync"
	"time"
)

type PushServer struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	//pushClients []*PushClient // 到所有 pushserver 的连接
	mu          sync.RWMutex
	pushClients map[string]*PushClient // 到所有 pushserver 的连接
	//userSessions sync.Map      // uid → *WSConn
	clog            *zap.Logger
	clientConnInter common.UserConnInter
}

func NewPushServer(clientConnInter common.UserConnInter) (*PushServer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	gs := &PushServer{
		ctx:             ctx,
		cancel:          cancel,
		clog:            logger.L,
		clientConnInter: clientConnInter,
		pushClients:     make(map[string]*PushClient),
	}
	clog := gs.clog
	err := gs.watchPushServer()
	if err != nil {
		clog.Error("watch push server error", zap.Error(err))
		return nil, err
	}
	return gs, nil
}

// 创建连接 并且上传所有的用户
func (gs *PushServer) watchPushServer() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	clog := gs.clog
	target := etcdapi.GetEtcdPushServerTarget()
	etcdCli := etcdx.GetGlobalLeaseEtcd()
	e, err := etcdCli.GetGRpcPointEndList(ctx, target)
	if err != nil {
		clog.Error("watch push server info failed", zap.Error(err))
		return err
	}

	for _, v := range e {
		addr := v.Addr
		_, ok := gs.pushClients[addr]
		if ok {
			continue
		}
		err = gs.createPushClient(addr)
		if err != nil {
			clog.Error("create push client failed", zap.Error(err))
			return err
		}
	}
	etcdCli.WatcherEndPointMgr(target, gs.handlePushEtcdInfoForEndPoint)
	return nil
}
func (gs *PushServer) createPushClient(address string) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	_, ok := gs.pushClients[address]
	if ok {
		return nil
	}
	clog := gs.clog
	gcfg := grpcx.DefaultClientConfig()
	gcfg.Target = address
	gcfg.TargetType = grpcx.TargetTypePassthrough
	etcdCli := etcdx.GetGlobalLeaseEtcd()
	gcli, err := grpcx.NewGrpcClient(gcfg, etcdCli.GetClient())
	if err != nil {
		clog.Error("watch room server info failed", zap.Error(err))
		return err
	}
	pushClient, err := NewPushClient(gcli.Conn(), gs)
	if err != nil {
		clog.Error("watch push server info failed", zap.Error(err))
		return err
	}
	gs.pushClients[address] = pushClient
	// 同步所有在线用户信息 todo
	uids := gs.clientConnInter.AllUids()
	clog.Debug("push server info", zap.Any("uids", uids))
	cfg := common.GetArgConfig()

	gateRegisterGrpcAddress := cfg.GRpcAddressRegister
	for _, uid := range uids {
		gs.OnUserOnline(uid, gateRegisterGrpcAddress)
	}
	return nil
}
func (gs *PushServer) handlePushEtcdInfoForEndPoint(key, address string, value any, opt endpoints.Operation) {
	clog := gs.clog
	clog.Debug("handlePushEtcdInfoForEndPoint", zap.String("key", key), zap.String("address", address), zap.Any("value", value), zap.Any("type", opt))

	switch opt {
	case endpoints.Add:
		err := gs.createPushClient(address)
		if err != nil {
			clog.Error("create push client failed", zap.Error(err))
		}
	case endpoints.Delete:
		addr := kit.ExtractAddrFromEtcdKey(key)
		clog.Info("handle game etcd info", zap.String("key", key), zap.String("addr", addr))
		gs.mu.Lock()
		delete(gs.pushClients, addr)
		gs.mu.Unlock()
	}
}

// OnUserOnline 用户上线，向所有 pushserver 上报 这里需要优化 一次性传入多个 TODO
func (gs *PushServer) OnUserOnline(uid uint64, address string) {
	for _, pc := range gs.pushClients {
		pc.ReportOnline(uid, address)
	}
}

// OnUserOffline 用户下线
func (gs *PushServer) OnUserOffline(uid uint64, reason string) {
	for _, pc := range gs.pushClients {
		pc.ReportOffline(uid, reason)
	}
}

// OnPushMessage 收到推送消息，转发给 websocket
func (gs *PushServer) OnPushMessage(msg *pb_service.PushToGate) {
	clog := gs.clog
	payload := msg.GetPayload()
	respFrame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_PUSH,
		FirstKind: pb_base.FirstKind_FIRST_PUSH,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}
	clog.Debug("[GatePushServer] Push frame", zap.Any("frame", respFrame))
	data, err := serializer.EncodeProto(respFrame)
	if err != nil {
		clog.Error("[GatePushServer] Serialize frame", zap.Error(err))
		return
	}
	pushUids := msg.GetUids()
	if len(pushUids) == 0 {
		// 全量广播
		gs.clientConnInter.BroadcastAll(data)
	} else {
		err = gs.clientConnInter.BatchPushAsync(pushUids, data)
		if err != nil {
			clog.Warn("[GatePushServer] BatchPushAsync err", zap.Error(err))
		}
	}
	clog.Debug("[GatePushServer] Push response", zap.Any("uids", pushUids))
}

func (gs *PushServer) Close() {
	gs.cancel()
	clog := gs.clog
	clog.Info("[GatePushServer] game grpc client pool shutdown complete start")
	for _, pc := range gs.pushClients {
		pc.Close()
	}
	clog.Info("[GatePushServer] game grpc client pool shutdown complete start 2")
	gs.wg.Wait()
	gs.clog.Info("[GatePushServer] gate grpc client pool shutdown complete")
}
