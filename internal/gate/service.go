package gate

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"github.com/k8s/muyi/shared/kit"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/k8s/muyi/internal/gate/client_conn"
	"github.com/k8s/muyi/internal/gate/grpc_client"
	"github.com/k8s/muyi/internal/gate/grpc_server"
	"github.com/k8s/muyi/internal/gate/hub"
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
	cfg      config.Gate
	redisCli *rediscli.Client
	hub      *hub.Hub
	gamePool *grpc_client.GamePoolMgr
	grpcSrv  *grpc.Server
	httpSrv  *http.Server // 这个是否有必要
	gateAddr string       // 当前gate对外地址，redis存储使用
	grpcAddr string
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	clog     *zap.Logger
}

func NewGateService(cfg config.Gate, gateAddr, grpcAddr string) *GateService {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &GateService{
		cfg:      cfg,
		redisCli: rediscli.GetClient(),
		hub:      hub.NewHub(),
		gamePool: grpc_client.NewGamePoolMgr(cfg.GrpcPoolSize),
		gateAddr: gateAddr,
		grpcAddr: grpcAddr,
		ctx:      ctx,
		cancel:   cancel,
		clog:     logger.L,
	}
	// 注册客户端帧回调
	//client_conn.NewClientConn()
	client_conn.RegisterFrameHandler(svc.handleWsFrame)
	return svc
}

// Start 启动WS服务 + GRPC推送服务
func (s *GateService) Start() error {
	// 1. 启动gate grpc服务（game调用推送）
	s.grpcSrv = grpc.NewServer()
	pushSrv := grpc_server.NewGatePushServer(s.hub)
	pb_service.RegisterGatePushServer(s.grpcSrv, pushSrv)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		lis, err := net.Listen("tcp", fmt.Sprintf("%s", s.grpcAddr))
		if err != nil {
			panic(err)
		}
		err = s.grpcSrv.Serve(lis)
		if err != nil {
			s.clog.Error("grpc server start error", zap.Error(err))
			os.Exit(1)
		}
	}()

	// 2. 启动websocket http服务
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.wsHandler)
	httpSrv := &http.Server{
		Addr:    fmt.Sprintf("%s", s.gateAddr),
		Handler: mux,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := httpSrv.ListenAndServe()
		if err != nil {
			s.clog.Error("http server start error", zap.Error(err))
			os.Exit(1)
		}
	}()
	s.httpSrv = httpSrv
	return nil
}

// wsHandler 客户端websocket连接入口
func (s *GateService) wsHandler(w http.ResponseWriter, r *http.Request) {
	uidStr := r.URL.Query().Get("uid")
	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil {
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// 新建客户端连接
	cli := client_conn.NewClientConn(ws, uid, s.gateAddr, s.redisCli, s.cfg)
	// 注册到hub
	if !s.hub.AddConn(uid, cli) {
		cli.Close()
		return
	}
	// 首次写入redis在线信息
	_ = s.SetUserOnline(uid, s.gateAddr, s.cfg.RedisExpire)
}

// todo
func (s *GateService) SetUserOnline(uid uint64, gateAddr string, expireTime int) error {
	return nil
}

// handleWsFrame 处理客户端上行WsFrame，转发GameServer
func (s *GateService) handleWsFrame(frame *pb_base.WsFrame) {
	defer func() {
		if r := recover(); r != nil {
			s.clog.Error("handleWsFrame panic", zap.Any("r", r))
		}
	}()
	// 只处理客户端请求
	if frame.FrameType != pb_base.FrameType_FRAME_REQUEST {
		return
	}
	uid := frame.Uid
	roomId := frame.RoomId
	// 模拟：根据roomId获取对应gameserver地址（业务自行实现room -> game pod路由）
	gameAddr := s.getGameAddrByRoom(roomId)
	if gameAddr == "" {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "game server not found")
		return
	}
	// 获取game grpc client
	gameCli, err := s.gamePool.GetClient(gameAddr)
	s.clog.Debug("get game client", zap.String("gameAddr", gameAddr), zap.Error(err))
	if err != nil {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "connect game fail")
		return
	}
	reqBody := frame.GetPayload()
	reqBodyPro := &pb_base.ReqBody{}
	err = serializer.DecodeProto(reqBody, reqBodyPro)
	if err != nil {
		s.clog.Error("decode req body fail", zap.Error(err))
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, "decode req body fail")
		return
	}
	// 构造转发请求
	forwardReq := &pb_service.ForwardReq{
		Req: reqBodyPro,
	}
	// 传递上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		cconst.GRpcContextFieldUID, kit.Uint64ToString(uid),
	)
	ctx = context.WithValue(ctx, cconst.GRpcContextFieldUID, uid) // 服务内使用
	// grpc调用GameLogic转发
	rsp, err := gameCli.ForwardClientMsg(ctx, forwardReq)
	s.clog.Debug("forward client msg", zap.Any("rsp", rsp), zap.Error(err))
	if err != nil {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_ERROR, err.Error())
		return
	}
	// 封装响应WsFrame回写给客户端
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
		s.clog.Error("encode resp frame fail", zap.Error(err))
	}
	s.clog.Debug("encode resp frame data", zap.Any("frame", respFrame))
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
		s.clog.Error("encode resp fail", zap.Error(err))
		return
	}
	cli, ok := s.hub.GetConn(uid)
	if ok {
		err = cli.WriteMsg(data)
		if err != nil {
			s.clog.Error("write resp fail", zap.Error(err))
		}
	}
}

// getGameAddrByRoom 根据roomId路由game pod（k8s statefulset分片逻辑）
func (s *GateService) getGameAddrByRoom(roomId uint64) string {
	// 示例格式 game-0.game-service.default.svc.cluster.local:50051
	//return "game-0.game-service.default.svc.cluster.local:50051"
	return "172.16.111.60:9000"
}

// Shutdown 优雅关闭gate所有资源
func (s *GateService) Shutdown(ctx context.Context) error {
	s.cancel()
	// 关闭grpc服务
	s.grpcSrv.GracefulStop()
	// 关闭hub所有ws连接
	s.hub.Shutdown()
	// 关闭game grpc连接池
	s.gamePool.Shutdown()
	// 等待所有协程退出
	s.wg.Wait()
	s.httpSrv.Shutdown(ctx)
	// 关闭redis
	//_ = s.redisCli.Close()
	return nil
}
