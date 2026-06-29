package server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/push/common"
	"github.com/k8s/muyi/internal/push/grpc_server"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"os"
	"sync"
	"time"
)

type ServerInfo struct {
	grpcAddress         string
	registerGrpcAddress string
	target              string
}
type PushService struct {
	grpcSrv *grpc.Server
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	etcdCli *etcdx.LeaseEtcdClient
	sInfo   ServerInfo
	clog    *zap.Logger
}

func NewPushService() *PushService {
	ctx, cancel := context.WithCancel(context.Background())
	clog := logger.L
	etcdCli := etcdx.GetGlobalLeaseEtcd()
	obj := &PushService{}
	obj.etcdCli = etcdCli
	obj.ctx = ctx
	obj.cancel = cancel
	obj.clog = clog
	addr := common.GetArgConfig().GRpcAddr
	target := etcdx.GetEtcdPushServerTarget()
	registerAddr := common.GetArgConfig().RegisterAddr
	info := ServerInfo{}
	info.grpcAddress = addr
	info.registerGrpcAddress = registerAddr
	info.target = target
	obj.sInfo = info
	return obj
}

func (s *PushService) Start() error {
	clog := s.clog
	kaep := keepalive.EnforcementPolicy{
		MinTime:             30 * time.Second, // 允许客户端最快每 10s 发一次 PING（默认 5min 很严格）
		PermitWithoutStream: true,             // 允许无 RPC 时也发 PING（关键！如果客户端开了这个，这里必须也开）
	}
	s.grpcSrv = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
	)
	svr := grpc_server.NewPushServer()
	pb_service.RegisterPushServiceServer(s.grpcSrv, svr)
	addr := s.sInfo.grpcAddress
	target := s.sInfo.target
	registerAddr := s.sInfo.registerGrpcAddress
	err := s.registerInfoForRpcEndPoint(target, registerAddr) // 注册etcd
	if err != nil {
		clog.Error(err.Error())
		return err
	}
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
func (s *PushService) registerInfoForRpcEndPoint(target, address string) error {
	clog := s.clog
	info := &etcdx.EtcdServerInfo{}
	info.Addr = address
	info.Target = target
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.etcdCli.RegisterGrpcServerInfo(ctx, info)
	if err != nil {
		clog.Error("register game service failed", zap.Error(err))
		return err
	}
	return nil
}
func (s *PushService) Shutdown() {
	s.cancel()
	s.grpcSrv.GracefulStop()
	target := s.sInfo.target
	addr := s.sInfo.registerGrpcAddress
	err := s.etcdCli.UnRegisterGrpcServerInfo(target, addr)
	if err != nil {
		s.clog.Error("unregister game service failed", zap.Error(err))
	}
	s.wg.Wait()
	s.clog.Debug("push service shutdown success")
}
