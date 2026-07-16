package server

import (
	"context"
	"github.com/k8s/muyi/api/etcdapi"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/push/common"
	"github.com/k8s/muyi/internal/push/grpc_server"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"sync"
	"time"
)

type ServerInfo struct {
	grpcAddress         string
	registerGrpcAddress string
	target              string
}
type PushService struct {
	grpcSrv    *grpc.Server
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	etcdCli    *etcdx.LeaseEtcdClient
	sInfo      ServerInfo
	clog       *zap.Logger
	pushServer *grpc_server.PushServer
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
	addr := common.GetArgCfg().GRpcAddr
	infocfg := common.GetBaseCfg().ServerInfo
	target := etcdapi.GetEtcdPushServerTarget(infocfg.ProjectName, infocfg.Env)
	registerAddr := common.GetArgCfg().RegisterAddr
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
		MinTime:             30 * time.Second,
		PermitWithoutStream: true,
	}
	s.grpcSrv = grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
	)
	svr, err := grpc_server.NewPushServer(10)
	if err != nil {
		clog.Error("grpc server init error", zap.Error(err))
		return err
	}
	s.pushServer = svr
	pb_service.RegisterPushServiceServer(s.grpcSrv, svr)
	addr := s.sInfo.grpcAddress
	target := s.sInfo.target
	registerAddr := s.sInfo.registerGrpcAddress
	err = s.registerInfoForRpcEndPoint(target, registerAddr)
	if err != nil {
		clog.Error(err.Error())
		return err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}
		clog.Info("grpc server start listen", zap.String("addr", addr))
		err = s.grpcSrv.Serve(lis)
		if err != nil {
			clog.Warn("grpc serve exited", zap.Error(err))
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

//	func (s *PushService) Shutdown() {
//		clog := s.clog
//		clog.Info("=============================PushService Shutdown BEGIN=============================================")
//		s.pushServer.Close()
//		// 【关键1】先停止grpc服务，再cancel上下文，顺序不能颠倒
//		// 设置10秒超时，防止永久阻塞
//		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
//		defer stopCancel()
//
//		ch := make(chan struct{}, 1)
//		go func() {
//			clog.Info("begin grpc GracefulStop")
//			s.grpcSrv.GracefulStop()
//			ch <- struct{}{}
//		}()
//
//		select {
//		case <-ch:
//			clog.Info("=============================PushService Shutdown2 grpc GracefulStop complete=============================================")
//		case <-stopCtx.Done():
//			// 超时还有僵死连接，强制断开所有连接
//			clog.Warn("GracefulStop timeout, force stop grpc server")
//			s.grpcSrv.Stop()
//		}
//
//		// 【关键2】再取消全局上下文，关闭内部所有业务协程
//		s.cancel()
//
//		// 反注册etcd节点
//		target := s.sInfo.target
//		addr := s.sInfo.registerGrpcAddress
//		err := s.etcdCli.UnRegisterGrpcServerInfo(target, addr)
//		if err != nil {
//			s.clog.Error("unregister game service failed", zap.Error(err))
//		} else {
//			s.clog.Debug("unregister game service success")
//		}
//
//		// 等待主Serve协程退出
//		s.wg.Wait()
//		s.clog.Debug("push service shutdown success")
//	}
func (s *PushService) Shutdown() {
	clog := s.clog
	clog.Info("=============================PushService Shutdown BEGIN=============================================")

	// ======【最重要调换顺序】先关闭gRPC，让所有双向流自动退出 ======
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()

	ch := make(chan struct{}, 1)
	go func() {
		clog.Info("begin grpc GracefulStop")
		//s.grpcSrv.GracefulStop()
		s.grpcSrv.Stop()
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		clog.Info("=============================PushService Shutdown2 grpc GracefulStop complete=============================================")
	case <-stopCtx.Done():
		clog.Warn("GracefulStop timeout, force stop grpc server")
		s.grpcSrv.Stop()
	}

	// ====== gRPC优雅关闭完成后，再关闭业务worker ======
	s.pushServer.Close()

	s.cancel()

	// 反注册etcd
	target := s.sInfo.target
	addr := s.sInfo.registerGrpcAddress
	err := s.etcdCli.UnRegisterGrpcServerInfo(target, addr)
	if err != nil {
		s.clog.Error("unregister game service failed", zap.Error(err))
	} else {
		s.clog.Debug("unregister game service success")
	}

	s.wg.Wait()
	s.clog.Debug("push service shutdown success")
}
