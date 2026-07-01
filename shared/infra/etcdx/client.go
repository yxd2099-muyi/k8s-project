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
	"sync"
	"time"
)

// 全局常量统一定义
const (
	DefaultLeaseTTL   = 1200 // 默认20分钟租约
	OpTimeout         = 5 * time.Second
	WatchRetryBaseMs  = 1000
	WatchRetryMaxMs   = 5000
	RebuildLeaseSleep = 5 * time.Second
)

// 全局私有变量，单例实例 + once 锁
var (
	leaseEtcdInstance *LeaseEtcdClient
	once              sync.Once
)

// EtcdServerInfo  服务信息
type EtcdServerInfo struct {
	Target string // target 存储前缀 也是service
	Addr   string // 服务的地址 192.168.179.88:9090
	Meta   any
}

// LeaseEtcdClient 带统一共享租约的etcd客户端
type LeaseEtcdClient struct {
	client *clientv3.Client

	// 租约相关
	leaseID       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	leaseTTL      int64

	// 协程防重标记（解决多协程刷屏核心新增字段）
	mu          sync.Mutex
	loopRunning bool // 心跳协程是否正在运行

	// regCache：仅缓存当前进程主动Register注册的key
	regCache               sync.Map // key:string  value:string
	etcdGrpcServerManager  sync.Map // key:service -> manager
	etcdEndPointServerInfo sync.Map // key -> *EtcdServerInfo

	log          *zap.Logger
	globalCtx    context.Context
	globalCancel context.CancelFunc
	wg           sync.WaitGroup
}

func GetGlobalLeaseEtcd() *LeaseEtcdClient {
	return leaseEtcdInstance
}

func InitGlobalLeaseEtcd() (*LeaseEtcdClient, error) {
	var err error
	once.Do(func() {
		leaseEtcdInstance, err = NewLeaseEtcdClient()
	})
	return leaseEtcdInstance, err
}

// NewLeaseEtcdClient 创建带共享租约的etcd客户端（单进程全局一个实例）
func NewLeaseEtcdClient() (*LeaseEtcdClient, error) {
	etcdConfig := config.GlobalConf.Etcd
	cliCfg := clientv3.Config{
		Endpoints:            etcdConfig.Endpoints,
		DialTimeout:          time.Duration(etcdConfig.DialTimeout) * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		Username:             etcdConfig.Username,
		Password:             etcdConfig.Password,
	}
	cli, err := clientv3.New(cliCfg)
	if err != nil {
		logger.L.Error("create etcd raw client failed", zap.Error(err))
		return nil, fmt.Errorf("new etcd client err: %w", err)
	}

	// 兜底租约时长
	leaseTTL := int64(etcdConfig.LeaseTTL)
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

	// 创建租约
	if err = lec.createLease(); err != nil {
		_ = lec.Close()
		return nil, err
	}

	// 启动唯一心跳协程
	if err = lec.startKeepAliveStream(); err != nil {
		_ = lec.Close()
		return nil, err
	}

	lec.log.Info("lease etcd client init success", zap.Int64("leaseTTL", leaseTTL))
	return lec, nil
}

func (lec *LeaseEtcdClient) GetClient() *clientv3.Client {
	return lec.client
}

// createLease 创建共享租约
func (lec *LeaseEtcdClient) createLease() error {
	ctx, cancel := context.WithTimeout(lec.globalCtx, OpTimeout)
	defer cancel()
	resp, err := lec.client.Lease.Grant(ctx, lec.leaseTTL)
	if err != nil {
		return fmt.Errorf("lease grant err: %w", err)
	}
	lec.leaseID = resp.ID
	lec.log.Debug("create shared lease success", zap.Int64("leaseID", int64(lec.leaseID)), zap.Int64("ttl", lec.leaseTTL))
	return nil
}

// startKeepAliveStream 【优化】防止重复启动心跳协程
func (lec *LeaseEtcdClient) startKeepAliveStream() error {
	lec.mu.Lock()
	if lec.loopRunning {
		lec.mu.Unlock()
		return nil // 已有运行中的协程，不再新建
	}
	lec.loopRunning = true
	lec.mu.Unlock()

	ch, err := lec.client.Lease.KeepAlive(lec.globalCtx, lec.leaseID)
	if err != nil {
		lec.mu.Lock()
		lec.loopRunning = false
		lec.mu.Unlock()
		return fmt.Errorf("keep alive create err: %w", err)
	}
	lec.keepAliveChan = ch

	lec.wg.Add(1)
	go lec.keepAliveLoop()
	return nil
}

// keepAliveLoop 唯一心跳协程，极简消费逻辑，避免队列堆积
func (lec *LeaseEtcdClient) keepAliveLoop() {
	defer func() {
		if r := recover(); r != nil {
			lec.log.Error("keepAliveLoop panic", zap.Any("panic", r))
		}
		// 标记协程已退出
		lec.mu.Lock()
		lec.loopRunning = false
		lec.mu.Unlock()

		lec.wg.Done()
		lec.log.Warn("keep alive loop exit")
	}()

	for {
		select {
		case <-lec.globalCtx.Done():
			return
		case _, ok := <-lec.keepAliveChan:
			if !ok {
				// 如果正在关闭，直接放弃重建租约
				select {
				case <-lec.globalCtx.Done():
					return
				default:
				}

				lec.log.Warn("lease keep alive channel closed, start rebuild lease")
				if err := lec.rebuildLeaseAndReloadRegKeys(); err != nil {
					lec.log.Error("rebuild lease failed", zap.Error(err))
					time.Sleep(RebuildLeaseSleep)
					continue
				}
				lec.log.Info("rebuild lease and reload all register keys success")
				continue
			}
		}
	}
}

// rebuildLeaseAndReloadRegKeys 重建租约+重注册，增加关闭判断
func (lec *LeaseEtcdClient) rebuildLeaseAndReloadRegKeys() error {
	// 服务已经开始关闭，不再重建租约
	if lec.globalCtx.Err() != nil {
		return fmt.Errorf("service shutting down, skip rebuild lease")
	}

	if err := lec.createLease(); err != nil {
		return err
	}
	if err := lec.startKeepAliveStream(); err != nil {
		return err
	}

	var regErr error
	lec.regCache.Range(func(k, v any) bool {
		key := k.(string)
		val := v.(string)
		ctx, cancel := context.WithTimeout(lec.globalCtx, OpTimeout)
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
		val, ok := v.(*EtcdServerInfo)
		if !ok {
			return true
		}
		ctx, cancel := context.WithTimeout(lec.globalCtx, OpTimeout)
		defer cancel()
		if err := lec.RegisterGrpcServerInfo(ctx, val); err != nil {
			lec.log.Error("re-register key failed", zap.String("key", key), zap.Error(err))
			endpointErr = err
		}
		return true
	})
	return endpointErr
}

// Register 注册KV，绑定共享租约，存入本地注册缓存
func (lec *LeaseEtcdClient) Register(ctx context.Context, key, value string) error {
	_, err := lec.client.Put(ctx, key, value, clientv3.WithLease(lec.leaseID))
	if err != nil {
		return fmt.Errorf("register put err: %w", err)
	}
	lec.regCache.Store(key, value)
	lec.log.Debug("register key success", zap.String("key", key), zap.String("value", value))
	return nil
}

// UnRegister 注销本机注册key
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

// Exist 判断key是否存在
func (lec *LeaseEtcdClient) Exist(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, OpTimeout)
	defer cancel()
	res, err := lec.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get exist err: %w", err)
	}
	return len(res.Kvs) > 0, nil
}

// Get 通用查询单个key
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

// PrefixGet 前缀批量查询
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

// WatchPrefix 监听指定前缀，带指数退避重试
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
			select {
			case <-lec.globalCtx.Done():
				return
			default:
			}
			time.Sleep(time.Duration(retryDelay) * time.Millisecond)
		}
	}()
}

// Close 优雅关闭
func (lec *LeaseEtcdClient) Close() error {
	lec.log.Info("start close lease etcd client")
	// 1. 发出关闭信号，终止所有循环
	lec.globalCancel()
	// 2. 等待所有协程全部退出
	lec.wg.Wait()

	// 3. 主动撤销租约
	revokeCtx, revokeCancel := context.WithTimeout(context.Background(), OpTimeout)
	defer revokeCancel()
	if _, err := lec.client.Lease.Revoke(revokeCtx, lec.leaseID); err != nil {
		lec.log.Warn("revoke lease failed", zap.Int64("leaseID", int64(lec.leaseID)), zap.Error(err))
	}

	// 4. 关闭底层连接
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
	clog := lec.log
	target := info.Target
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		clog.Error("get endpoint manager failed", zap.String("target", target), zap.Error(err))
		return err
	}
	addr := info.Addr
	key := lec.getEndPointKey(target, addr)
	err = mgr.AddEndpoint(ctx, key, endpoints.Endpoint{Addr: addr, Metadata: info.Meta}, clientv3.WithLease(lec.leaseID))
	if err != nil {
		clog.Error("grpc server register failed", zap.String("target", target), zap.String("addr", addr))
		return err
	}
	lec.etcdEndPointServerInfo.Store(key, info)
	clog.Info("grpc server register success", zap.String("target", target), zap.String("addr", addr))
	return nil
}

// GetEndPointMgr 如果没有创建
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
	err = mgr.DeleteEndpoint(ctx, key)
	return err
}

func (lec *LeaseEtcdClient) GetGRpcPointEndList(ctx context.Context, target string) (endpoints.Key2EndpointMap, error) {
	mgr, err := lec.GetEndPointMgr(target)
	if err != nil {
		return nil, err
	}
	return mgr.List(ctx)
}

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
				time.Sleep(time.Duration(retryDelay) * time.Millisecond)
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
