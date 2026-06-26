package etcdx

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
)

func (lec *LeaseEtcdClient) RegisterGrpcServerInfo(ctx context.Context, info *EtcdServerInfo) error {
	if info == nil {
		return fmt.Errorf("info is nil")
	}
	clog := lec.log
	target := info.Target
	mgr, exist, err := lec.GetEndPointMgr(target)
	if err != nil {
		clog.Error("get endpoint manager failed", zap.String("target", target), zap.Error(err))
		return err
	}
	if !exist {
		mgr, err = endpoints.NewManager(lec.client, target)
		if err != nil {
			clog.Error("create endpoint manager failed", zap.String("target", target), zap.Error(err))
			return err
		}
		lec.etcdGrpcServerManager.Store(target, mgr)
	}
	addr := info.Addr
	clog.Info("grpc server register success", zap.String("target", target), zap.String("addr", addr))
	key := lec.getEndPointKey(target, addr)
	err = mgr.AddEndpoint(ctx, key, endpoints.Endpoint{Addr: addr, Metadata: info.Meta}, clientv3.WithLease(lec.leaseID))
	if err != nil {
		clog.Error("grpc server register failed", zap.String("target", target), zap.String("addr", addr))
		return err
	}
	lec.etcdEndPointServerInfo.Store(key, info)
	return nil
}
func (lec *LeaseEtcdClient) GetEndPointMgr(target string) (endpoints.Manager, bool, error) {
	v, ok := lec.etcdGrpcServerManager.Load(target)
	if !ok {
		return nil, false, nil
	}
	mgr, ok := v.(endpoints.Manager)
	if !ok {
		return nil, false, fmt.Errorf("grpc server manager store invalid")
	}
	return mgr, true, nil
}
func (lec *LeaseEtcdClient) getEndPointKey(target, address string) string {
	key := fmt.Sprintf("%s/%s", target, address)
	return key
}
func (lec *LeaseEtcdClient) UnRegisterGrpcServerInfo(target, address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), OpTimeout)
	defer cancel()
	key := lec.getEndPointKey(target, address)
	lec.etcdGrpcServerManager.Delete(target)
	lec.etcdEndPointServerInfo.Delete(key)
	mgr, exist, err := lec.GetEndPointMgr(target)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	err = mgr.DeleteEndpoint(ctx, key)
	return err
}
func (lec *LeaseEtcdClient) GetGRpcPointEndList(ctx context.Context, target string) (e endpoints.Key2EndpointMap, err error) {

	mgr, exist, err := lec.GetEndPointMgr(target)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	e, err = mgr.List(ctx)
	return
}
func (lec *LeaseEtcdClient) WatcherEndPointMgr(target string, handler cconst.UpdateEtcdEndPointGrpcHandler) {
	clog := lec.log
	mgr, exist, err := lec.GetEndPointMgr(target)
	if err != nil {
		clog.Error("get endpoint manager failed", zap.String("target", target), zap.Error(err))
		return
	}
	if !exist {
		clog.Info("endpoint manager not found", zap.String("target", target))
		return
	}

	lec.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				clog.Error("watch prefix panic", zap.Any("panic", r))
			}
			lec.wg.Done()
			clog.Warn("WatcherEndPointMgr exit")
		}()
		for {
			// 全局上下文关闭，直接退出循环
			select {
			case <-lec.globalCtx.Done():
				return
			default:
			}
			ch, er := mgr.NewWatchChannel(lec.globalCtx)
			if er != nil {
				clog.Error("watch channel failed", zap.Error(er))
				return
			}
			for updates := range ch {
				for _, u := range updates {
					ep := u.Endpoint
					address := ep.Addr
					meta := ep.Metadata
					handler(address, meta, u.Op)
				}
			}
		}
	}()
}
