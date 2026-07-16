package etcdx

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultLeaseTTL    = 1200
	OpTimeout          = 8 * time.Second
	WatchRetryBaseMs   = 1000
	WatchRetryMaxMs    = 5000
	RebuildLeaseSleep  = 5 * time.Second
	LeaseGrantRetryMax = 3
)

var (
	leaseEtcdInstance *LeaseEtcdClient
	once              sync.Once
)

type EtcdServerInfo struct {
	Target string
	Addr   string
	Meta   any
}

type LeaseEtcdClient struct {
	client *clientv3.Client

	mu          sync.Mutex
	leaseID     clientv3.LeaseID
	leaseTTL    int64
	loopRunning bool

	regCache               sync.Map
	etcdGrpcServerManager  sync.Map
	etcdEndPointServerInfo sync.Map

	log          *zap.Logger
	globalCtx    context.Context
	globalCancel context.CancelFunc
	wg           sync.WaitGroup
}

func GetGlobalLeaseEtcd() *LeaseEtcdClient {
	return leaseEtcdInstance
}

func InitGlobalLeaseEtcd(etcdCfg config.Etcd) (*LeaseEtcdClient, error) {
	var err error
	once.Do(func() {
		leaseEtcdInstance, err = NewLeaseEtcdClient(etcdCfg)
	})
	return leaseEtcdInstance, err
}

func NewLeaseEtcdClient(etcdCfg config.Etcd) (*LeaseEtcdClient, error) {
	cliCfg := clientv3.Config{
		Endpoints:            etcdCfg.Endpoints,
		DialTimeout:          time.Duration(etcdCfg.DialTimeout) * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		Username:             etcdCfg.Username,
		Password:             etcdCfg.Password,
	}
	cli, err := clientv3.New(cliCfg)
	if err != nil {
		logger.L.Error("create etcd raw client failed", zap.Error(err))
		return nil, fmt.Errorf("new etcd client err: %w", err)
	}

	leaseTTL := int64(etcdCfg.LeaseTTL)
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}

	globalCtx, globalCancel := context.WithCancel(context.Background())
	lec := &LeaseEtcdClient{
		client:       cli,
		leaseTTL:     leaseTTL,
		log:          logger.L,
		globalCtx:    globalCtx,
		globalCancel: globalCancel,
	}

	if err = lec.createLeaseWithRetry(); err != nil {
		_ = lec.Close()
		return nil, err
	}

	lec.startKeepAliveLoop()
	lec.log.Info("lease etcd client init success", zap.Int64("leaseTTL", leaseTTL))
	return lec, nil
}

func (lec *LeaseEtcdClient) GetClient() *clientv3.Client {
	return lec.client
}

// createLeaseWithRetry 带重试创建租约
func (lec *LeaseEtcdClient) createLeaseWithRetry() error {
	var lastErr error
	for retry := 0; retry < LeaseGrantRetryMax; retry++ {
		err := lec.createLease()
		if err == nil {
			return nil
		}
		lastErr = err
		lec.log.Warn("LeaseGrant failed, prepare retry", zap.Int("retry", retry+1), zap.Error(err))
		sleepMs := (retry + 1) * 1000
		// 可中断休眠（若上下文取消则提前返回）
		if !lec.sleepWithContext(time.Duration(sleepMs+rand.Intn(500)) * time.Millisecond) {
			return fmt.Errorf("context canceled during retry sleep")
		}
	}
	return fmt.Errorf("lease grant all retries failed: %w", lastErr)
}

// createLease 创建共享租约
func (lec *LeaseEtcdClient) createLease() error {
	lec.mu.Lock()
	defer lec.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), OpTimeout)
	defer cancel()

	resp, err := lec.client.Lease.Grant(ctx, lec.leaseTTL)
	if err != nil {
		return fmt.Errorf("lease grant err: %w", err)
	}
	lec.leaseID = resp.ID
	lec.log.Debug("create shared lease success", zap.Int64("leaseID", int64(lec.leaseID)), zap.Int64("ttl", lec.leaseTTL))
	return nil
}

// startKeepAliveLoop 启动唯一心跳主循环
func (lec *LeaseEtcdClient) startKeepAliveLoop() {
	lec.mu.Lock()
	if lec.loopRunning {
		lec.mu.Unlock()
		return
	}
	lec.loopRunning = true
	lec.mu.Unlock()

	lec.wg.Add(1)
	go lec.keepAliveMainLoop()
}

// keepAliveMainLoop 主循环：每次断开都重建租约+新建心跳流
func (lec *LeaseEtcdClient) keepAliveMainLoop() {
	defer func() {
		if r := recover(); r != nil {
			lec.log.Error("keepAliveMainLoop panic", zap.Any("panic", r))
		}
		lec.mu.Lock()
		lec.loopRunning = false
		lec.mu.Unlock()
		lec.wg.Done()
		lec.log.Warn("keep alive main loop exit")
	}()

	for {
		select {
		case <-lec.globalCtx.Done():
			return
		default:
		}

		lec.mu.Lock()
		leaseID := lec.leaseID
		lec.mu.Unlock()

		keepChan, err := lec.client.Lease.KeepAlive(lec.globalCtx, leaseID)
		if err != nil {
			lec.log.Error("create keep alive stream failed, will rebuild lease", zap.Error(err))
			// 可中断休眠
			if !lec.sleepWithContext(RebuildLeaseSleep) {
				return
			}
			_ = lec.rebuildLeaseAndReloadRegKeys()
			continue
		}

		// 消费心跳channel，通道关闭则跳出
		for range keepChan {
		}

		// 流关闭，进入重建流程
		lec.log.Warn("lease keep alive stream closed, start rebuild lease")
		// 【关键】可中断休眠，等待冷却
		if !lec.sleepWithContext(RebuildLeaseSleep) {
			return
		}
		if err := lec.rebuildLeaseAndReloadRegKeys(); err != nil {
			lec.log.Error("rebuild lease failed", zap.Error(err))
		} else {
			lec.log.Info("rebuild lease and reload all register keys success")
		}
	}
}

// sleepWithContext 可中断睡眠，返回 false 表示上下文已取消
func (lec *LeaseEtcdClient) sleepWithContext(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-lec.globalCtx.Done():
		return false
	}
}

// rebuildLeaseAndReloadRegKeys 重建租约+重注册（加入重试机制）
func (lec *LeaseEtcdClient) rebuildLeaseAndReloadRegKeys() error {
	if lec.globalCtx.Err() != nil {
		return fmt.Errorf("service shutting down, skip rebuild lease")
	}

	if err := lec.createLeaseWithRetry(); err != nil {
		return err
	}

	var regErr error
	lec.regCache.Range(func(k, v any) bool {
		key := k.(string)
		val := v.(string)
		ctx, cancel := context.WithTimeout(context.Background(), OpTimeout)
		defer cancel()
		if err := lec.Register(ctx, key, val); err != nil {
			lec.log.Error("re-register key failed", zap.String("key", key), zap.Error(err))
			regErr = err
		}
		return true
	})
	if regErr != nil {
		return regErr
	}

	var endpointErr error
	lec.etcdEndPointServerInfo.Range(func(k, v any) bool {
		key := k.(string)
		info, ok := v.(*EtcdServerInfo)
		if !ok {
			return true
		}
		ctx, cancel := context.WithTimeout(context.Background(), OpTimeout)
		defer cancel()
		if err := lec.RegisterGrpcServerInfo(ctx, info); err != nil {
			lec.log.Error("re-register grpc endpoint failed", zap.String("key", key), zap.Error(err))
			endpointErr = err
		}
		return true
	})
	return endpointErr
}

// Register 注册KV，绑定共享租约
func (lec *LeaseEtcdClient) Register(ctx context.Context, key, value string) error {
	lec.mu.Lock()
	leaseID := lec.leaseID
	lec.mu.Unlock()

	_, err := lec.client.Put(ctx, key, value, clientv3.WithLease(leaseID))
	if err != nil {
		return fmt.Errorf("register put err: %w", err)
	}
	lec.regCache.Store(key, value)
	lec.log.Debug("register key success", zap.String("key", key), zap.String("value", value))
	return nil
}

func (lec *LeaseEtcdClient) UnRegister(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, OpTimeout)
	defer cancel()
	_, err := lec.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("unregister delete err: %w", err)
	}
	lec.regCache.Delete(key)
	lec.log.Debug("unregister key success", zap.String("key", key))
	return nil
}

func (lec *LeaseEtcdClient) Exist(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, OpTimeout)
	defer cancel()
	res, err := lec.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get exist err: %w", err)
	}
	return len(res.Kvs) > 0, nil
}

func (lec *LeaseEtcdClient) Get(ctx context.Context, key string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, OpTimeout)
	defer cancel()
	resp, err := lec.client.Get(ctx, key)
	if err != nil {
		return "", false, fmt.Errorf("get key err: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	return string(resp.Kvs[0].Value), true, nil
}

func (lec *LeaseEtcdClient) PrefixGet(ctx context.Context, prefix string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, OpTimeout)
	defer cancel()
	resp, err := lec.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("prefix get err: %w", err)
	}
	ret := make(map[string]string, len(resp.Kvs))
	for _, item := range resp.Kvs {
		ret[string(item.Key)] = string(item.Value)
	}
	return ret, nil
}

// WatchPrefix 监听指定前缀，带指数退避重试（可中断）
func (lec *LeaseEtcdClient) WatchPrefix(prefix string, handler cconst.UpdateEtcdHandler) {
	lec.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				lec.log.Error("watch prefix panic", zap.String("prefix", prefix), zap.Any("panic", r))
			}
			lec.wg.Done()
			lec.log.Warn("watch prefix exit", zap.String("prefix", prefix))
		}()

		retryDelay := WatchRetryBaseMs
		for {
			select {
			case <-lec.globalCtx.Done():
				return
			default:
			}

			watchChan := lec.client.Watch(lec.globalCtx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
			valid := false
			for resp := range watchChan {
				valid = true
				if resp.Err() != nil {
					lec.log.Error("watch receive error", zap.String("prefix", prefix), zap.Error(resp.Err()))
					break
				}
				for _, ev := range resp.Events {
					k := string(ev.Kv.Key)
					v := string(ev.Kv.Value)
					if ev.Type == clientv3.EventTypeDelete && ev.PrevKv != nil {
						v = string(ev.PrevKv.Value)
					}
					handler(k, v, ev.Type)
				}
			}

			if !valid {
				retryDelay = min(retryDelay*2, WatchRetryMaxMs)
			} else {
				retryDelay = WatchRetryBaseMs
			}
			// 可中断退避
			if !lec.sleepWithContext(time.Duration(retryDelay) * time.Millisecond) {
				return
			}
		}
	}()
}

// Close 优雅关闭
func (lec *LeaseEtcdClient) Close() error {
	lec.log.Info("start close lease etcd client")
	lec.globalCancel()
	lec.wg.Wait()

	lec.mu.Lock()
	leaseID := lec.leaseID
	lec.mu.Unlock()

	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), OpTimeout)
	defer revokeCancel()
	if _, err := lec.client.Lease.Revoke(revokeCtx, leaseID); err != nil {
		lec.log.Warn("revoke lease failed", zap.Int64("leaseID", int64(leaseID)), zap.Error(err))
	}

	err := lec.client.Close()
	if err != nil {
		lec.log.Error("close raw etcd client failed", zap.Error(err))
		return err
	}
	lec.log.Info("lease etcd client closed complete")
	return nil
}

func (lec *LeaseEtcdClient) RegisterGrpcServerInfo(ctx context.Context, info *EtcdServerInfo) error {
	if info == nil {
		return fmt.Errorf("info is nil")
	}
	target := info.Target
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		return err
	}

	lec.mu.Lock()
	leaseID := lec.leaseID
	lec.mu.Unlock()

	addr := info.Addr
	key := lec.getEndPointKey(target, addr)
	err = mgr.AddEndpoint(ctx, key, endpoints.Endpoint{Addr: addr, Metadata: info.Meta}, clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}
	lec.etcdEndPointServerInfo.Store(key, info)
	lec.log.Info("grpc server register success", zap.String("target", target), zap.String("addr", addr))
	return nil
}

func (lec *LeaseEtcdClient) GetEndPointMgr(target string) (endpoints.Manager, error) {
	v, ok := lec.etcdGrpcServerManager.Load(target)
	if !ok {
		mgr, err := endpoints.NewManager(lec.client, target)
		if err != nil {
			return nil, err
		}
		lec.etcdGrpcServerManager.Store(target, mgr)
		return mgr, nil
	}
	mgr, _ := v.(endpoints.Manager)
	return mgr, nil
}

func (lec *LeaseEtcdClient) getEndPointKey(target, address string) string {
	return fmt.Sprintf("%s/%s", target, address)
}

func (lec *LeaseEtcdClient) UnRegisterGrpcServerInfo(target, address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), OpTimeout)
	defer cancel()
	key := lec.getEndPointKey(target, address)
	lec.etcdEndPointServerInfo.Delete(key)
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		return err
	}
	return mgr.DeleteEndpoint(ctx, key)
}

func (lec *LeaseEtcdClient) GetGRpcPointEndList(ctx context.Context, target string) (endpoints.Key2EndpointMap, error) {
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		return nil, err
	}
	return mgr.List(ctx)
}

// WatcherEndPointMgr 监听端点变化（可中断）
func (lec *LeaseEtcdClient) WatcherEndPointMgr(target string, handler cconst.UpdateEtcdEndPointGrpcHandler) {
	clog := lec.log
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		clog.Error("get endpoint manager failed", zap.String("target", target), zap.Error(err))
		return
	}

	lec.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				clog.Error("watch endpoint panic", zap.Any("panic", r))
			}
			lec.wg.Done()
			clog.Warn("WatcherEndPointMgr exit")
		}()

		retryDelay := WatchRetryBaseMs
		for {
			select {
			case <-lec.globalCtx.Done():
				return
			default:
			}

			ch, er := mgr.NewWatchChannel(lec.globalCtx)
			if er != nil {
				clog.Error("create watch channel err", zap.Error(er))
				// 可中断退避
				if !lec.sleepWithContext(time.Duration(retryDelay) * time.Millisecond) {
					return
				}
				retryDelay = min(retryDelay*2, WatchRetryMaxMs)
				continue
			}
			retryDelay = WatchRetryBaseMs

			for updates := range ch {
				for _, u := range updates {
					ep := u.Endpoint
					handler(u.Key, ep.Addr, ep.Metadata, u.Op)
				}
			}
		}
	}()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
