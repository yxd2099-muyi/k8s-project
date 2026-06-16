package etcdx

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k8s/muyi/shared/infra/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"sync"
	"time"
)

// Registry 服务注册
type Registry struct {
	client          *clientv3.Client
	leaseID         clientv3.LeaseID
	keepAliveChan   <-chan *clientv3.LeaseKeepAliveResponse
	prefix          string
	key             string
	value           string
	leaseTTL        int64
	stopSignal      chan struct{}
	runMu           sync.Mutex
	isRunning       bool
	keepAliveCancel context.CancelFunc
	clog            *zap.Logger
}

func NewRegistry(etcdConfig config.Etcd, prefix string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            etcdConfig.Endpoints,
		DialTimeout:          time.Duration(etcdConfig.DialTimeout) * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		Username:             etcdConfig.Username,
		Password:             etcdConfig.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client failed: %w", err)
	}

	ttl := int64(DefaultLeaseTTL)
	if etcdConfig.TTL > 0 {
		ttl = int64(etcdConfig.TTL)

	}

	return &Registry{
		client:     cli,
		leaseTTL:   ttl,
		prefix:     prefix,
		stopSignal: make(chan struct{}),
	}, nil
}

// Register 注册服务，增加运行状态互斥锁防并发调用
func (r *Registry) Register(instance *ServiceInstance) error {
	r.runMu.Lock()
	if r.isRunning {
		r.runMu.Unlock()
		return fmt.Errorf("registry already running, cannot register repeatedly")
	}
	r.runMu.Unlock()

	key := GetKey(r.prefix, instance.Name, instance.ID)
	instance.Key = key
	data, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("marshal service instance failed: %w", err)
	}
	r.value = string(data)
	r.key = key

	// 创建租约
	ctxGrant, cancelGrant := context.WithTimeout(context.Background(), DefaultReqTimeout)
	defer cancelGrant()
	leaseResp, err := r.client.Grant(ctxGrant, r.leaseTTL)
	if err != nil {
		return fmt.Errorf("grant lease fail: %w", err)
	}
	r.leaseID = leaseResp.ID

	// 写入etcd
	ctxPut, cancelPut := context.WithTimeout(context.Background(), DefaultReqTimeout)
	defer cancelPut()
	_, err = r.client.Put(ctxPut, r.key, r.value, clientv3.WithLease(r.leaseID))
	if err != nil {
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, _ = r.client.Revoke(revokeCtx, r.leaseID)
		revokeCancel()
		return fmt.Errorf("put service key fail: %w", err)
	}

	// 管控keepAlive上下文
	kaCtx, kaCancel := context.WithCancel(context.Background())
	r.keepAliveCancel = kaCancel
	keepChan, err := r.client.KeepAlive(kaCtx, r.leaseID)
	if err != nil {
		kaCancel()
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, _ = r.client.Revoke(revokeCtx, r.leaseID)
		revokeCancel()
		return fmt.Errorf("keepalive create fail: %w", err)
	}
	r.keepAliveChan = keepChan

	r.runMu.Lock()
	r.isRunning = true
	r.runMu.Unlock()

	r.clog.Info("service register success",
		zap.String("prefix", r.prefix),
		zap.String("key", r.key),
		zap.Int64("leaseID", int64(r.leaseID)),
		zap.Int64("leaseTTL", r.leaseTTL))

	go r.listenKeepAlive()
	return nil
}

// listenKeepAlive 续约监听，panic后重注册，通道关闭触发重试
func (r *Registry) listenKeepAlive() {
	defer func() {
		if re := recover(); r != nil {
			r.clog.Error("listenKeepAlive panic", zap.Any("panic", re))
			if err := r.reRegister(); err != nil {
				r.clog.Error("panic reRegister failed", zap.Error(err))
			}
		}
	}()

	var lastLogTime time.Time
	const logInterval = 5 * time.Minute
	for {
		select {
		case <-r.stopSignal:
			return
		case res, ok := <-r.keepAliveChan:
			if !ok {
				r.clog.Warn("keepalive channel closed, trigger re-register")
				if err := r.reRegister(); err != nil {
					r.clog.Error("re-register after channel close fail", zap.Error(err))
				}
				continue
			}

			if time.Since(lastLogTime) > logInterval {
				r.clog.Debug("keepalive heartbeat normal",
					zap.String("key", r.key),
					zap.Int64("leaseID", int64(res.ID)),
					zap.Int64("ttl_remain", res.TTL))
				lastLogTime = time.Now()
			}
		}
	}
}

// innerRenewLease 抽离租约重建逻辑，消除goto跨变量声明报错
func (r *Registry) innerRenewLease() error {
	// 释放旧keepAlive与租约
	if r.keepAliveCancel != nil {
		r.keepAliveCancel()
	}
	if r.leaseID != 0 {
		ctxRevoke, cancelRevoke := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, _ = r.client.Revoke(ctxRevoke, r.leaseID)
		cancelRevoke()
		r.leaseID = 0
	}

	// 申请新租约
	ctxGrant, cancelGrant := context.WithTimeout(context.Background(), DefaultReqTimeout)
	leaseResp, err := r.client.Grant(ctxGrant, r.leaseTTL)
	cancelGrant()
	if err != nil {
		return fmt.Errorf("grant new lease err: %w", err)
	}

	// 写入key
	ctxPut, cancelPut := context.WithTimeout(context.Background(), DefaultReqTimeout)
	_, err = r.client.Put(ctxPut, r.key, r.value, clientv3.WithLease(leaseResp.ID))
	cancelPut()
	if err != nil {
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, _ = r.client.Revoke(revokeCtx, leaseResp.ID)
		revokeCancel()
		return fmt.Errorf("put new lease key err: %w", err)
	}

	// 新建keepalive
	kaCtx, kaCancel := context.WithCancel(context.Background())
	keepChan, err := r.client.KeepAlive(kaCtx, leaseResp.ID)
	if err != nil {
		kaCancel()
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, _ = r.client.Revoke(revokeCtx, leaseResp.ID)
		revokeCancel()
		return fmt.Errorf("create keepalive err: %w", err)
	}

	r.leaseID = leaseResp.ID
	r.keepAliveChan = keepChan
	r.keepAliveCancel = kaCancel
	r.clog.Info("service re-register single attempt success",
		zap.String("key", r.key),
		zap.Int64("newLeaseID", int64(r.leaseID)))
	return nil
}

// reRegister 重注册：回收旧租约、可中断重试、阶梯退避有上限
func (r *Registry) reRegister() error {
	r.runMu.Lock()
	if !r.isRunning {
		r.runMu.Unlock()
		return fmt.Errorf("registry already stopped, skip re-register")
	}
	r.runMu.Unlock()

	var lastErr error
	backoff := BackoffStep
	for i := 0; i < MaxReRegisterRetry; i++ {
		if r.client == nil {
			return fmt.Errorf("etcd client nil")
		}

		err := r.innerRenewLease()
		if err == nil {
			return nil
		}
		lastErr = err

		r.clog.Warn("re-register single attempt failed",
			zap.Int("attempt", i+1),
			zap.Duration("backoff", backoff),
			zap.Error(lastErr))

		// 可中断sleep，收到停止信号直接退出
		select {
		case <-r.stopSignal:
			return fmt.Errorf("registry stopping, abort re-register")
		case <-time.After(backoff):
		}
		if backoff*2 < MaxBackoffInterval {
			backoff *= 2
		}
	}

	return fmt.Errorf("re-register exceed max retry %d, last err: %w", MaxReRegisterRetry, lastErr)
}

// Unregister 优雅注销，修正执行顺序
func (r *Registry) Unregister() error {
	r.runMu.Lock()
	if !r.isRunning {
		r.runMu.Unlock()
		return nil
	}
	r.isRunning = false
	r.runMu.Unlock()

	close(r.stopSignal)

	// 1. 关闭keepAlive上下文
	if r.keepAliveCancel != nil {
		r.keepAliveCancel()
	}

	// 2. 撤销租约（自动删除etcd key，无需手动Delete）
	if r.leaseID != 0 {
		ctxRevoke, cancelRevoke := context.WithTimeout(context.Background(), DefaultReqTimeout)
		_, err := r.client.Revoke(ctxRevoke, r.leaseID)
		cancelRevoke()
		if err != nil {
			r.clog.Error("revoke lease failed",
				zap.String("key", r.key),
				zap.Int64("leaseID", int64(r.leaseID)),
				zap.Error(err))
		}
	}

	// 3. 关闭etcd client
	closeErr := r.client.Close()
	r.client = nil
	r.clog.Info("registry unregister complete", zap.String("key", r.key))
	return closeErr
}
