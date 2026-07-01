package gate

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/k8s/muyi/api/etcdapi"
	"github.com/k8s/muyi/api/model"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/common"
	"github.com/k8s/muyi/internal/gate/conn"
	"github.com/k8s/muyi/internal/gate/grpc_server"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/grpcx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"github.com/k8s/muyi/shared/kit"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type GateService struct {
	cfg               config.Gate
	redisCli          *rediscli.Client
	hub               *conn.ConnManager
	grpcSrv           *grpc.Server
	httpSrv           *http.Server
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	clog              *zap.Logger
	roomServerInfoMap sync.Map // key: address,value : GameNode

	etcdCli *etcdx.LeaseEtcdClient

	gClient         *grpcx.GrpcClient
	gameLogicClient pb_service.GameLogicClient //pb_service.NewGameLogicClient(conn)
	pusServer       *grpc_server.PushServer
}

func NewGateService(cfg config.Gate) *GateService {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &GateService{
		cfg:      cfg,
		redisCli: rediscli.GetClient(),
		hub:      conn.NewConnMgr(),
		ctx:      ctx,
		cancel:   cancel,
		clog:     logger.L,
	}
	conn.RegisterFrameHandler(svc.handleWsFrame)
	return svc
}

func (s *GateService) wathRoomServerInfoEndPoint() error {
	// 初始化以下
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	clog := s.clog
	target := etcdapi.GetEtcdRoomServerTarget()
	e, err := s.etcdCli.GetGRpcPointEndList(ctx, target)
	if err != nil {
		s.clog.Error("watch room server info failed", zap.Error(err))
		return err
	}
	for _, v := range e {
		addr := v.Addr
		meta := v.Metadata // 本质是个json 字符串
		clog.Debug("watch room server info", zap.String("addr", addr), zap.Any("meta", meta))
		var info model.GameNode
		err = serializer.EncodeDecodeJson(meta, &info)
		if err != nil {
			clog.Error("watch room server info failed", zap.Error(err))
			continue
		}

		clog.Info("watch room server info", zap.Any("info", info))
		s.roomServerInfoMap.Store(addr, &info)
	}
	s.etcdCli.WatcherEndPointMgr(target, s.handleGameEtcdInfoForGrpcEndPoint)
	return nil
}

// Start 启动WS服务 + GRPC推送服务
func (s *GateService) Start() error {
	clog := s.clog
	etcdCli := etcdx.GetGlobalLeaseEtcd()
	s.etcdCli = etcdCli
	pushS, err := grpc_server.NewPushServer(s.hub)
	s.pusServer = pushS
	err = s.wathRoomServerInfoEndPoint()
	if err != nil {
		clog.Error("watch room server info failed", zap.Error(err))
		return err
	}
	// grpc ConnClient
	gcfg := grpcx.DefaultClientConfig()
	target := etcdapi.GetEtcdRoomServerTarget()
	gcfg.Target = target
	gcfg.TargetType = grpcx.TargetTypeEtcd
	gcfg.LBPolicy = string(cconst.LBTargetDirect)
	gcli, err := grpcx.NewGrpcClient(gcfg, etcdCli.GetClient())
	if err != nil {
		clog.Error("watch room server info failed", zap.Error(err))
		return err
	}
	s.gClient = gcli
	clog.Info("watch room server info", zap.Any("gcli", gcli))
	s.gameLogicClient = pb_service.NewGameLogicClient(gcli.Conn())
	// 1. 启动gate grpc服务（game调用推送）
	s.grpcSrv = grpc.NewServer()
	pushSrv := grpc_server.NewGatePushServer(s.hub)
	pb_service.RegisterGatePushServer(s.grpcSrv, pushSrv)
	s.wg.Add(1)
	cfg := common.GetArgConfig()
	grpcAddr := cfg.GRpcAddress
	go func() {
		defer s.wg.Done()
		//lis, err := net.Listen("tcp", s.grpcAddr)
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			s.clog.Error("grpc server listen failed", zap.String("addr", grpcAddr), zap.Error(err))
			return // 移除os.Exit，交给上层Shutdown处理
		}
		s.clog.Info("gate grpc server listen success", zap.String("addr", grpcAddr))
		err = s.grpcSrv.Serve(lis)
		if err != nil && err != grpc.ErrServerStopped {
			s.clog.Error("grpc server serve error", zap.Error(err))
		}
		s.clog.Info("gate grpc server exited")
	}()

	// 2. 启动websocket http服务
	wsAddress := cfg.WsAddress
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.wsHandler)
	s.httpSrv = &http.Server{
		Addr:    wsAddress,
		Handler: mux,
		// 增加http服务超时，防止连接永久挂住
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.clog.Info("gate http ws server listen start", zap.String("addr", wsAddress))
		err := s.httpSrv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			s.clog.Error("http ws server listen failed", zap.Error(err))
		}
		s.clog.Info("gate http ws server exited")
	}()
	return nil
}

// wsHandler 客户端websocket连接入口
func (s *GateService) wsHandler(w http.ResponseWriter, r *http.Request) {
	// gate全局关闭时拒绝新连接
	select {
	case <-s.ctx.Done():
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	default:
	}

	uidStr := r.URL.Query().Get("uid")
	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.clog.Warn("websocket upgrade fail", zap.Uint64("uid", uid), zap.Error(err))
		return
	}
	cfg := common.GetArgConfig()
	grpcAddrRegister := cfg.GRpcAddressRegister
	//
	cli := conn.NewClientConn(ws, uid, grpcAddrRegister, s.redisCli, s.cfg, s.hub, s.pusServer)
	if !s.hub.ReplaceConn(uid, cli) {
		cli.Close()
		return
	}
}

// handleWsFrame 处理客户端上行WsFrame，转发GameServer
func (s *GateService) handleWsFrame(frame *pb_base.WsFrame) {
	defer func() {
		if r := recover(); r != nil {
			s.clog.Error("handleWsFrame panic recover", zap.Any("panic", r), zap.Uint64("uid", frame.Uid), zap.Any("roomId", frame.RoomId))
		}
	}()
	if frame.FrameType != pb_base.FrameType_FRAME_REQUEST {
		return
	}
	uid := frame.Uid
	roomId := frame.RoomId

	gameAddr, found := s.getGameAddrByRoom(roomId)
	if gameAddr == "" || !found {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "game server not found by roomId")
		return
	}

	//gameCli, err := s.gamePool.GetClient(gameAddr)
	//if err != nil {
	//	s.clog.Warn("get game grpc client fail", zap.String("gameAddr", gameAddr), zap.Any("roomId", roomId), zap.Error(err))
	//	s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "connect game server failed")
	//	return
	//}

	reqBody := frame.GetPayload()
	reqBodyPro := &pb_base.ReqBody{}
	err := serializer.DecodeProto(reqBody, reqBodyPro)
	if err != nil {
		s.clog.Error("decode client req body fail", zap.Uint64("uid", uid), zap.Error(err))
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "request proto decode error")
		return
	}

	forwardReq := &pb_service.ForwardReq{
		Req: reqBodyPro,
	}

	// grpc调用超时控制
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		cconst.GRpcContextFieldUID, kit.Uint64ToString(uid),
		cconst.ContextFieldRouterTargetAddress, gameAddr,
	)
	ctx = context.WithValue(ctx, cconst.GRpcContextFieldUID, uid)
	var rsp *pb_service.ForwardRsp
	gameCli := s.gameLogicClient
	clog := s.clog
	clog.Debug("client req frame", zap.Uint64("uid", uid), zap.String("req", string(reqBody)), zap.Any("gameAddr", gameAddr))
	if roomId > 0 {
		rsp, err = gameCli.ForwardClientRoomMsg(ctx, forwardReq)
	} else {
		rsp, err = gameCli.ForwardClientMsg(ctx, forwardReq)
	}
	if err != nil {
		s.clog.Warn("forward client msg to game fail", zap.Uint64("uid", uid), zap.String("gameAddr", gameAddr), zap.Error(err))
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, err.Error())
		return
	}

	respFrame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_RESPONSE,
		FirstKind: frame.FirstKind,
		Seq:       frame.Seq,
		Uid:       uid,
		ErrCode:   rsp.Code,
		ErrMsg:    rsp.Msg,
		Payload:   rsp.GetBody().GetPayload(),
		Timestamp: frame.Timestamp,
		RoomId:    roomId,
	}
	data, err := serializer.EncodeProto(respFrame)
	if err != nil {
		s.clog.Error("encode response ws frame fail", zap.Uint64("uid", uid), zap.Error(err))
		return
	}

	cli, ok := s.hub.GetConn(uid)
	if ok {
		_ = cli.WriteMsg(data)
	}
}

// sendErrResp 下发错误响应
func (s *GateService) sendErrResp(origin *pb_base.WsFrame, uid uint64, code pb_base.ErrCode, msg string) {
	respFrame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_RESPONSE,
		Seq:       origin.GetSeq(),
		Uid:       uid,
		ErrCode:   code,
		ErrMsg:    msg,
		RoomId:    origin.GetRoomId(),
	}
	data, err := serializer.EncodeProto(respFrame)
	if err != nil {
		s.clog.Error("encode error resp frame fail", zap.Uint64("uid", uid), zap.Error(err))
		return
	}
	cli, ok := s.hub.GetConn(uid)
	if ok {
		if err := cli.WriteMsg(data); err != nil {
			s.clog.Warn("write error resp to client fail", zap.Uint64("uid", uid), zap.Error(err))
		}
	}
}

// getGameAddrByRoom 根据roomId路由game pod，可替换为redis分片实现
func (s *GateService) getGameAddrByRoom(roomId uint32) (string, bool) {
	// 生产环境替换为redis分片路由逻辑
	var targetAddr string
	found := false
	s.roomServerInfoMap.Range(func(k, v interface{}) bool {
		addr, ok := k.(string)
		if !ok {
			return true
		}
		node, ok := v.(*model.GameNode)
		if !ok || node == nil {
			return true
		}
		if roomId >= node.RoomMin && roomId <= node.RoomMax {
			targetAddr = addr
			found = true
			// 返回false，直接终止Range遍历，节省性能
			return false
		}
		return true
	})
	//targetAddr = "172.16.111.60:9000"
	//found = true
	return targetAddr, found
}
func (s *GateService) handleGameEtcdInfoForGrpcEndPoint(key, address string, value any, opt endpoints.Operation) {
	clog := s.clog
	s.clog.Debug("handle game etcd info", zap.String("key", key), zap.String("address", address), zap.Any("value", value), zap.Any("type", opt))
	clog.Debug("handleGameEtcdInfoForGrpcEndPoint", zap.String("addr", address), zap.Any("meta", value))
	var info model.GameNode
	err := serializer.EncodeDecodeJson(value, &info)
	if err != nil {
		clog.Error("handleGameEtcdInfoForGrpcEndPoint", zap.Error(err))
		return
	}
	switch opt {
	case endpoints.Add:
		s.roomServerInfoMap.Store(address, &info)
	case endpoints.Delete:
		addr := kit.ExtractAddrFromEtcdKey(key)
		clog.Info("handle game etcd info", zap.String("key", key), zap.String("addr", addr))
		s.roomServerInfoMap.Delete(addr)
	}
}

func (s *GateService) handleGameEtcdInfo(key, value string, eType mvccpb.Event_EventType) {

	var podInfo model.GameNode
	s.clog.Debug("handle game etcd info", zap.String("key", key), zap.String("value", value), zap.Any("type", eType))
	err := serializer.DecodeJsonForString(value, &podInfo)
	if err != nil {
		s.clog.Error("decode game etcd info fail", zap.String("key", key), zap.String("value", value))
		return
	}
	address := podInfo.GrpcAddr
	switch eType {
	case clientv3.EventTypePut:
		s.roomServerInfoMap.Store(address, &podInfo)
	case clientv3.EventTypeDelete:
		s.roomServerInfoMap.Delete(address)
	}
}

// Shutdown 优雅关闭gate所有资源，增加超时兜底、修复关闭顺序、解决wg死锁
// 传入外层ctx用于全局进程关闭超时控制
func (s *GateService) Shutdown(ctx context.Context) error {
	const shutdownMaxWait = 15 * time.Second
	// 步骤1：发送全局关闭信号，所有子协程感知
	s.cancel()
	s.clog.Info("Shutdown step1: trigger global cancel signal")

	// 步骤2：优先关闭HTTP WS服务，拒绝新客户端连接、断开存量ws长连接
	httpShutdownCtx, httpCancel := context.WithTimeout(ctx, 5*time.Second)
	defer httpCancel()
	if err := s.httpSrv.Shutdown(httpShutdownCtx); err != nil {
		s.clog.Error("Shutdown step2: http server shutdown error", zap.Error(err))
	} else {
		s.clog.Info("Shutdown step2: http ws server shutdown complete")
	}

	// 步骤3：关闭Gate GRPC服务（Game侧推送gate的入口）
	s.grpcSrv.GracefulStop()
	s.clog.Info("Shutdown step3: gate grpc server graceful stop complete")

	// 步骤4：关闭所有在线客户端ws连接管理器
	s.hub.Shutdown()
	s.clog.Info("Shutdown step4: all client ws connections closed")

	s.clog.Info("Shutdown step5: game grpc client pool shutdown complete")
	s.gClient.Close()
	s.clog.Info("Shutdown step6: game grpc client pool shutdown complete")
	s.pusServer.Close()
	s.clog.Info("Shutdown step7: game grpc client pool shutdown complete")
	// 步骤6：设置超时等待所有wg协程退出，解决永久阻塞卡死
	waitDone := make(chan struct{}, 1)
	go func() {
		s.wg.Wait()
		waitDone <- struct{}{}
	}()

	select {
	case <-waitDone:
		s.clog.Info("Shutdown step6: all background goroutine exited normally")
	case <-time.After(shutdownMaxWait):
		s.clog.Error("Shutdown step6: wait wg timeout, some goroutine stuck, force exit")
		return fmt.Errorf("shutdown wait wg timeout %s", shutdownMaxWait)
	case <-ctx.Done():
		s.clog.Error("Shutdown step6: outer context canceled while waiting wg")
		return ctx.Err()
	}
	s.clog.Info("Shutdown step7: all background goroutine exited normally")

	return nil
}
