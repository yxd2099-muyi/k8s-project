package server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/grpc_server"
	"github.com/k8s/muyi/internal/game/k8s"
	"github.com/k8s/muyi/internal/game/push"
	"github.com/k8s/muyi/internal/game/room"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
	"time"
)

type GameService struct {
	cfg         config.Game
	roomMgr     *room.RoomMgr
	rangeCalc   *k8s.RoomRangeCalc
	pushMgr     *push.PushManager
	grpcSrv     *grpc.Server
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	etcdCli     *etcdx.LeaseEtcdClient
	grpcEtcdKey string
	target      string
	address     string
	clog        *zap.Logger
}

func NewGameService() (*GameService, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化房间分片计算
	cfg := config.GetGameServerCfg()
	clog := logger.L
	rangeCalc, err := k8s.NewRoomRangeCalc(uint32(cfg.MaxRoomNum))
	if err != nil {
		cancel()
		clog.Error("create game service failed")
		return nil, err
	}
	etcdCli := etcdx.GetGlobalLeaseEtcd()

	gameCfg := config.GetGameServerCfg()

	svc := &GameService{
		cfg:       gameCfg,
		roomMgr:   room.NewRoomMgr(gameCfg.MaxRoomNum),
		rangeCalc: rangeCalc,
		pushMgr:   push.InitGlobalPushMgr(),
		ctx:       ctx,
		cancel:    cancel,
		clog:      clog,
		etcdCli:   etcdCli,
	}

	return svc, nil
}
func (s *GameService) registerInfoForRpcEndPoint() error {
	podRoomInfo := s.rangeCalc.GetGameNode()
	clog := s.clog
	clog.Info("game service init success", zap.Any("pod_room_info", podRoomInfo))
	address := podRoomInfo.GrpcAddr
	info := &etcdx.EtcdServerInfo{}
	info.Addr = address
	info.Target = s.target
	info.Meta = &podRoomInfo
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.etcdCli.RegisterGrpcServerInfo(ctx, info)
	if err != nil {
		clog.Error("register game service failed", zap.Error(err))
		return err
	}
	return nil
}
func (s *GameService) registerPodInfo() error {
	podRoomInfo := s.rangeCalc.GetGameNode()
	clog := s.clog
	clog.Info("game service init success", zap.Any("pod_room_info", podRoomInfo))
	podRoomInfoStr, err := serializer.EncodeJson(&podRoomInfo)
	if err != nil {
		clog.Error("serializer game service failed", zap.Error(err))
		return err
	}
	etcdKey := etcdx.GetEtcdRoomServerKey(podRoomInfo.GrpcAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = s.etcdCli.Register(ctx, etcdKey, string(podRoomInfoStr))
	if err != nil {
		clog.Error("register game service failed", zap.Error(err))
		return err
	}
	s.grpcEtcdKey = etcdKey
	return nil
}
func (s *GameService) Start() error {
	s.grpcSrv = grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			common.ContextInterception(),
			// 可以添加其他拦截器
		),
	)
	logicSrv := grpc_server.NewGameLogicServer(s.roomMgr, s.rangeCalc)
	pb_service.RegisterGameLogicServer(s.grpcSrv, logicSrv)
	target := etcdx.GetEtcdRoomServerTarget()
	s.target = target
	err := s.registerInfoForRpcEndPoint() // 如果注册直接key, value 使用另外一种方式 todo
	if err != nil {
		s.clog.Error("register game service failed", zap.Error(err))
		return err
	}
	addr := common.GetArgConfig().GRpcAddr
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		//lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.Port))
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}
		err = s.grpcSrv.Serve(lis)
		if err != nil {
			os.Exit(1)
		}
	}()
	return nil
}

func (s *GameService) Shutdown() {
	s.cancel()
	s.grpcSrv.GracefulStop()
	s.roomMgr.Close()
	s.pushMgr.Shutdown()
	//s.etcdCli.UnRegister(context.Background(), s.grpcEtcdKey) // todo
	err := s.etcdCli.UnRegisterGrpcServerInfo(s.target, s.address)
	if err != nil {
		s.clog.Error("unregister game service failed", zap.Error(err))
	}
	s.wg.Wait()
	s.clog.Debug("game service shutdown success")
}
