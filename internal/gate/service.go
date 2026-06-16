package gate

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/common/frame"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/redisClient"
	"github.com/k8s/muyi/shared/kit/serializer"
	"net"
	"net/http"
	"strconv"
	"sync"
	//"github.com/k8s/muyi/internal/common"
	//"github.com/k8s/muyi/internal/common/frame"
	//"github.com/k8s/muyi/internal/common/rediscli"
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
	redisCli *redisClient.Client
	hub      *hub.Hub
	gamePool *grpc_client.GamePoolMgr
	grpcSrv  *grpc.Server
	httpSrv  *http.Server // 这个是否有必要
	gateAddr string       // 当前gate对外地址，redis存储使用
	grpcAddr string
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewGateService(cfg config.Gate, gateAddr, grpcAddr string) *GateService {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &GateService{
		cfg:      cfg,
		redisCli: redisClient.GetClient(),
		hub:      hub.NewHub(),
		gamePool: grpc_client.NewGamePoolMgr(cfg.GrpcPoolSize),
		gateAddr: gateAddr,
		grpcAddr: grpcAddr,
		ctx:      ctx,
		cancel:   cancel,
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
		lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.grpcAddr))
		if err != nil {
			panic(err)
		}
		_ = s.grpcSrv.Serve(lis)
	}()

	// 2. 启动websocket http服务
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.wsHandler)
	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.cfg.WsPort),
		Handler: mux,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = httpSrv.ListenAndServe()
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
	//_ = s.redisCli.SetUserOnline(uid, s.gateAddr, s.cfg.RedisExpire)
	_ = s.SetUserOnline(uid, s.gateAddr, s.cfg.RedisExpire)
}

// todo
func (s *GateService) SetUserOnline(uid uint64, gateAddr string, expireTime int) error {
	return nil
}

// handleWsFrame 处理客户端上行WsFrame，转发GameServer
func (s *GateService) handleWsFrame(frame *pb_base.WsFrame) {
	// 只处理客户端请求
	if frame.FrameType != pb_base.FrameType_FRAME_REQUEST {
		return
	}
	uid := frame.Uid
	roomId := frame.RoomId
	// 模拟：根据roomId获取对应gameserver地址（业务自行实现room -> game pod路由）
	gameAddr := s.getGameAddrByRoom(roomId)
	if gameAddr == "" {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_GAME_SERVER_UNAVAIL, "game server not found")
		return
	}
	// 获取game grpc client
	gameCli, err := s.gamePool.GetClient(gameAddr)
	if err != nil {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_INTERNAL_ERR, "connect game fail")
		return
	}
	// 构造转发请求
	forwardReq := &pb_service.ForwardReq{
		Req: &pb_base.ReqBody{
			Uid:     uid,
			RoomId:  roomId,
			Payload: frame.Payload,
		},
	}
	// grpc调用GameLogic转发
	rsp, err := gameCli.ForwardClientMsg(context.Background(), forwardReq)
	if err != nil {
		s.sendErrResp(frame, uid, pb_base.ErrCode_EC_INTERNAL_ERR, err.Error())
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
		Payload:   rsp.Body.Payload,
		Timestamp: frame.Timestamp,
		RoomId:    roomId,
	}
	//data, _ := frame.EncodeWsFrame(respFrame)
	data, _ := serializer.EncodeProto(respFrame)
	cli, ok := s.hub.GetConn(uid)
	if ok {
		_ = cli.WriteMsg(data)
	}
}

// getGameAddrByRoom 根据roomId路由game pod（k8s statefulset分片逻辑）
func (s *GateService) getGameAddrByRoom(roomId uint64) string {
	// 示例格式 game-0.game-service.default.svc.cluster.local:50051
	return "game-0.game-service.default.svc.cluster.local:50051"
}

// sendErrResp 下发错误响应
func (s *GateService) sendErrResp(origin *pb_base.WsFrame, uid uint64, code pb_base.ErrCode, msg string) {
	respFrame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_RESPONSE,
		Seq:       origin.Seq,
		Uid:       uid,
		ErrCode:   code,
		ErrMsg:    msg,
		RoomId:    origin.RoomId,
	}
	data, _ := frame.EncodeWsFrame(respFrame)
	cli, ok := s.hub.GetConn(uid)
	if ok {
		_ = cli.WriteMsg(data)
	}
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
