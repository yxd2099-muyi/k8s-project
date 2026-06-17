package server

import (
	"context"
	"fmt"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/game/common"
	"github.com/k8s/muyi/internal/game/grpc_server"
	"github.com/k8s/muyi/internal/game/internal/router"
	"github.com/k8s/muyi/internal/game/k8s"
	"github.com/k8s/muyi/internal/game/push"
	"github.com/k8s/muyi/internal/game/room"
	"github.com/k8s/muyi/shared/infra/config"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
)

type GameService struct {
	cfg       config.Game
	roomMgr   *room.RoomMgr
	rangeCalc *k8s.RoomRangeCalc
	pushMgr   *push.PushManager
	grpcSrv   *grpc.Server
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewGameService() (*GameService, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化房间分片计算
	rangeCalc, err := k8s.NewRoomRangeCalc()
	if err != nil {
		cancel()
		return nil, err
	}
	gameCfg := config.GetGameServerCfg()
	svc := &GameService{
		cfg:       gameCfg,
		roomMgr:   room.NewRoomMgr(gameCfg.MaxRoomNum),
		rangeCalc: rangeCalc,
		pushMgr:   push.NewPushManager(),
		ctx:       ctx,
		cancel:    cancel,
	}
	router.InitRouter()
	return svc, nil
}

func (s *GameService) Start() error {
	s.grpcSrv = grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			common.ContextInterception(),
			// 可以添加其他拦截器
		),
	)
	logicSrv := grpc_server.NewGameLogicServer(s.roomMgr, s.rangeCalc, s.pushMgr)
	pb_service.RegisterGameLogicServer(s.grpcSrv, logicSrv)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.Port))
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
	s.roomMgr.Shutdown()
	s.pushMgr.Shutdown()
	s.wg.Wait()
}
