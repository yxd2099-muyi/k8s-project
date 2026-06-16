package etcdx

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"sync"
	"time"
)

// Discovery 服务发现
type Discovery struct {
	client         *clientv3.Client
	handler        UpdateServiceHandler
	prefix         string
	serviceList    map[string]*ServiceInstance
	lock           sync.RWMutex
	stopSignal     chan struct{}
	watchCancel    context.CancelFunc
	snapshotTicker *time.Ticker
	wg             sync.WaitGroup
	clog           *zap.Logger
}

func NewDiscovery(etcdConfig *config.Etcd, prefix string, handler UpdateServiceHandler) (*Discovery, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdConfig.Endpoints,
		DialTimeout: time.Duration(etcdConfig.DialTimeout) * time.Second,
		Username:    etcdConfig.Username,
		Password:    etcdConfig.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client failed: %w", err)
	}

	d := &Discovery{
		client:         cli,
		prefix:         prefix,
		handler:        handler,
		serviceList:    make(map[string]*ServiceInstance),
		stopSignal:     make(chan struct{}),
		snapshotTicker: time.NewTicker(SnapshotSyncPeriod),
		clog:           logger.L,
	}

	// 先创建watch再初始化快照，消除时序间隙丢失事件
	watchCtx, watchCancel := context.WithCancel(context.Background())
	d.watchCancel = watchCancel

	// 全量加载快照
	if err := d.loadSnapshot(); err != nil {
		_ = cli.Close()
		return nil, err
	}

	d.wg.Add(2)
	go d.watchLoop(watchCtx)
	go d.snapshotLoop()

	return d, nil
}

// loadSnapshot 全量拉取服务快照，覆盖本地缓存
func (d *Discovery) loadSnapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
	defer cancel()

	resp, err := d.client.Get(ctx, d.prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("get prefix snapshot failed: %w", err)
	}

	newMap := make(map[string]*ServiceInstance)
	for _, kv := range resp.Kvs {
		var ins ServiceInstance
		if err := json.Unmarshal(kv.Value, &ins); err != nil {
			d.clog.Error("unmarshal snapshot instance fail",
				zap.String("key", string(kv.Key)),
				zap.Error(err))
			continue
		}
		newMap[string(kv.Key)] = &ins
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	d.serviceList = newMap
	d.clog.Info("reload service snapshot success",
		zap.String("prefix", d.prefix),
		zap.Int("service_count", len(newMap)))
	return nil
}

// snapshotLoop 定时全量同步快照，修复watch断连脏数据
func (d *Discovery) snapshotLoop() {
	defer d.wg.Done()
	for {
		select {
		case <-d.stopSignal:
			return
		case <-d.snapshotTicker.C:
			if err := d.loadSnapshot(); err != nil {
				d.clog.Error("timer sync snapshot failed",
					zap.String("prefix", d.prefix),
					zap.Error(err))
			}
		}
	}
}

// watchLoop 自动重连watch主循环
func (d *Discovery) watchLoop(watchCtx context.Context) {
	defer d.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			d.clog.Error("discovery watch panic recover", zap.Any("panic", r))
		}
	}()

	for {
		// 新建watch流
		watchChan := d.client.Watch(watchCtx, d.prefix, clientv3.WithPrefix())
		watchErr := false

	consumeLoop:
		for {
			select {
			case <-d.stopSignal:
				return
			case resp, ok := <-watchChan:
				if !ok || resp.Err() != nil {
					watchErr = true
					break consumeLoop
				}
				for _, ev := range resp.Events {
					switch ev.Type {
					case clientv3.EventTypePut:
						d.handlePut(string(ev.Kv.Key), string(ev.Kv.Value))
					case clientv3.EventTypeDelete:
						d.handleDelete(string(ev.Kv.Key))
					}
				}
			}
		}

		if !watchErr {
			return
		}
		// 通道异常，等待间隔后重建watch
		d.clog.Warn("watch stream broken, try reconnect", zap.String("prefix", d.prefix))
		select {
		case <-d.stopSignal:
			return
		case <-time.After(WatchRetryInterval):
		}
	}
}

func (d *Discovery) handlePut(key, value string) {
	var instance ServiceInstance
	if err := json.Unmarshal([]byte(value), &instance); err != nil {
		d.clog.Error("unmarshal put instance failed",
			zap.String("key", key),
			zap.String("prefix", d.prefix),
			zap.Error(err))
		return
	}

	// 锁内仅拷贝数据，解锁后执行业务handler，防止锁阻塞
	d.lock.Lock()
	d.serviceList[key] = &instance
	copyIns := *&instance
	d.lock.Unlock()

	d.clog.Info("service instance put",
		zap.String("key", key),
		zap.String("prefix", d.prefix),
		zap.Any("instance", copyIns))
	d.handler(ETCDUpdateTypePut, key, &copyIns)
}

func (d *Discovery) handleDelete(key string) {
	d.lock.Lock()
	ins, exist := d.serviceList[key]
	if exist {
		delete(d.serviceList, key)
	}
	d.lock.Unlock()

	if !exist {
		d.clog.Info("delete service instance not found",
			zap.String("key", key),
			zap.String("prefix", d.prefix))
		return
	}

	d.clog.Info("service instance delete",
		zap.String("key", key),
		zap.String("prefix", d.prefix),
		zap.Any("instance", ins))
	d.handler(ETCDUpdateTypeDelete, key, ins)
}

// GetServices 获取所有服务实例，合并删除冗余GetServicesAddress
func (d *Discovery) GetServices() []*ServiceInstance {
	d.lock.RLock()
	defer d.lock.RUnlock()

	list := make([]*ServiceInstance, 0, len(d.serviceList))
	for _, v := range d.serviceList {
		list = append(list, v)
	}
	return list
}

// Close 优雅关闭所有协程、释放资源
func (d *Discovery) Close() error {
	close(d.stopSignal)
	if d.watchCancel != nil {
		d.watchCancel()
	}
	d.snapshotTicker.Stop()
	d.wg.Wait()
	d.lock.Lock()
	d.serviceList = make(map[string]*ServiceInstance)
	d.lock.Unlock()
	return d.client.Close()
}
