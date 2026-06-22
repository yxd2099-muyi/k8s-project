package conn

import (
	"context"
	"errors"
	"fmt"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

// ClientConnInter 业务侧连接抽象接口，解耦避免循环依赖
type ClientConnInter interface {
	// WriteMsg 写入二进制消息，返回error代表发送失败
	WriteMsg(data []byte) error
	// Close 幂等关闭连接，重复调用无副作用
	Close()
}

// ConnManagerConfig 连接管理器配置
type ConnManagerConfig struct {
	WorkerNum          int           // 批量推送工作协程数量
	ShutdownTimeout    time.Duration // Shutdown优雅关闭超时，0=无限阻塞
	AutoKickOnPushFail bool          // 推送失败时自动下线用户
}

// DefaultConfig 默认配置
func DefaultConfig() ConnManagerConfig {
	return ConnManagerConfig{
		WorkerNum:          32,
		ShutdownTimeout:    30 * time.Second,
		AutoKickOnPushFail: true,
	}
}

// ConnManager 用户长连接管理器
// 基于sync.Map管理uid长连接，支持批量并发推送、全服广播、优雅关闭、监控埋点
type ConnManager struct {
	cfg    ConnManagerConfig
	logger *zap.Logger

	conns sync.Map    // key:uint64 uid, value:ClientConnInter
	shut  atomic.Bool // 全局关闭标记 true=停止，禁止新增连接

	// 工作池任务通道
	taskCh chan pushTask
	wg     sync.WaitGroup

	// 监控指标 原子操作
	onlineCount atomic.Uint64 // 当前在线连接总数
	pushFailCnt atomic.Uint64 // 推送失败总次数
	replaceCnt  atomic.Uint64 // 连接替换次数
	delCnt      atomic.Uint64 // 用户下线总次数
}

// pushTask 批量推送任务
type pushTask struct {
	uids []uint64
	data []byte
}

// NewConnMgr 使用默认配置创建管理器
func NewConnMgr() *ConnManager {
	return NewConnMgrWithConfig(DefaultConfig(), logger.L)
}

// NewConnMgrWithConfig 自定义配置+日志器创建管理器
func NewConnMgrWithConfig(cfg ConnManagerConfig, logger *zap.Logger) *ConnManager {
	if cfg.WorkerNum <= 0 {
		cfg.WorkerNum = DefaultConfig().WorkerNum
	}
	mgr := &ConnManager{
		cfg:    cfg,
		logger: logger,
		taskCh: make(chan pushTask, cfg.WorkerNum*4),
	}
	// 启动worker工作协程
	for i := 0; i < cfg.WorkerNum; i++ {
		mgr.wg.Add(1)
		go mgr.workerLoop()
	}
	return mgr
}

// workerLoop 推送工作协程循环消费任务
func (m *ConnManager) workerLoop() {
	defer m.wg.Done()
	for task := range m.taskCh {
		m.handlePushTask(task)
	}
}

// handlePushTask 处理单条批量推送任务
func (m *ConnManager) handlePushTask(task pushTask) {
	defer func() {
		if err := recover(); err != nil {
			m.logger.Error(fmt.Sprintf("batch push panic recover, err=%v", err))
		}
	}()
	// 拷贝消息切片，避免底层数组共享导致并发数据竞争
	dataCopy := make([]byte, len(task.data))
	copy(dataCopy, task.data)

	for _, uid := range task.uids {
		cli, ok := m.GetConn(uid)
		if !ok {
			continue
		}
		err := cli.WriteMsg(dataCopy)
		if err != nil {
			m.pushFailCnt.Add(1)
			m.logger.Error(fmt.Sprintf("push msg failed, uid=%d err=%s", uid, err.Error()))
			if m.cfg.AutoKickOnPushFail {
				m.DelConn(uid)
			}
		}
	}
}

// AddConn 注册用户连接
// return false: 管理器已关闭 / uid已存在；true=注册成功，计数+1
func (m *ConnManager) AddConn(uid uint64, cli ClientConnInter) bool {
	if m.shut.Load() {
		m.logger.Error(fmt.Sprintf("add conn failed, manager is shutdown, uid=%d", uid))
		return false
	}
	_, loaded := m.conns.LoadOrStore(uid, cli)
	if loaded {
		m.logger.Info(fmt.Sprintf("add conn succ, uid=%d", uid))
		return false
	}
	m.onlineCount.Add(1)
	m.logger.Info(fmt.Sprintf("add conn success, uid=%d online=%d", uid, m.GetOnlineNum()))
	return true
}

// ReplaceConn 强制替换用户连接（断线重连场景）
// 自动关闭旧连接，成功返回true；管理器关闭返回false
func (m *ConnManager) ReplaceConn(uid uint64, newCli ClientConnInter) bool {
	if m.shut.Load() {
		m.logger.Error(fmt.Sprintf("replace conn failed, manager shutdown, uid=%d", uid))
		return false
	}
	oldVal, loaded := m.conns.Swap(uid, newCli)
	if !loaded {
		// 无旧连接，新增在线计数
		m.onlineCount.Add(1)
		m.logger.Info(fmt.Sprintf("replace conn no old conn, add new uid=%d online=%d", uid, m.GetOnlineNum()))
	} else {
		// 关闭旧连接
		if oldCli := castClientConn(oldVal); oldCli != nil {
			oldCli.Close()
			m.replaceCnt.Add(1)
			m.logger.Info(fmt.Sprintf("replace conn close old client, uid=%d", uid))
		}
	}
	return true
}

// DelConn 删除并关闭用户连接，幂等操作
func (m *ConnManager) DelConn(uid uint64) {
	val, ok := m.conns.LoadAndDelete(uid)
	if !ok {
		return
	}
	cli := castClientConn(val)
	if cli == nil {
		return
	}
	cli.Close()
	m.onlineCount.Add(^uint64(0)) // 原子减1
	m.delCnt.Add(1)
	m.logger.Info(fmt.Sprintf("del conn success, uid=%d online=%d", uid, m.GetOnlineNum()))
}

// GetConn 获取用户连接
func (m *ConnManager) GetConn(uid uint64) (ClientConnInter, bool) {
	val, ok := m.conns.Load(uid)
	if !ok {
		return nil, false
	}
	cli := castClientConn(val)
	return cli, cli != nil
}

// BatchPushAsync 异步批量推送，送入worker池并发处理，不阻塞调用协程
// 海量uid推荐使用，自动限制并发协程数量
func (m *ConnManager) BatchPushAsync(uids []uint64, data []byte) error {
	if m.shut.Load() {
		return errors.New("conn manager already shutdown")
	}
	select {
	case m.taskCh <- pushTask{uids: uids, data: data}:
		return nil
	default:
		return errors.New("push task channel full, drop batch msg")
	}
}

// BatchPushSync 同步串行批量推送，直接当前协程执行，适合少量uid
func (m *ConnManager) BatchPushSync(uids []uint64, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	for _, uid := range uids {
		cli, ok := m.GetConn(uid)
		if !ok {
			continue
		}
		err := cli.WriteMsg(dataCopy)
		if err != nil {
			m.pushFailCnt.Add(1)
			m.logger.Error(fmt.Sprintf("sync push failed uid=%d err=%s", uid, err.Error()))
			if m.cfg.AutoKickOnPushFail {
				m.DelConn(uid)
			}
		}
	}
}

// BroadcastAll 全服广播所有在线用户
func (m *ConnManager) BroadcastAll(data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	m.conns.Range(func(k, v any) bool {
		uid := k.(uint64)
		cli := castClientConn(v)
		if cli == nil {
			return true
		}
		err := cli.WriteMsg(dataCopy)
		if err != nil {
			m.pushFailCnt.Add(1)
			m.logger.Error(fmt.Sprintf("broadcast push failed uid=%d err=%s", uid, err.Error()))
			if m.cfg.AutoKickOnPushFail {
				m.DelConn(uid)
			}
		}
		return true
	})
}

// IsShutdown 判断管理器是否关闭
func (m *ConnManager) IsShutdown() bool {
	return m.shut.Load()
}

// Shutdown 优雅关闭全部连接，阻塞等待清理完成
// 关闭后禁止新增连接、停止所有推送任务
func (m *ConnManager) Shutdown() error {
	// 标记关闭，拦截新增连接
	if m.shut.Swap(true) {
		return errors.New("conn manager already shutdown")
	}
	m.logger.Info("start shutdown conn manager")

	// 关闭任务通道，停止接收新推送任务
	close(m.taskCh)

	// 等待所有正在执行的推送任务完成
	ctx := context.Background()
	if m.cfg.ShutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.cfg.ShutdownTimeout)
		defer cancel()
	}
	doneCh := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		m.logger.Info("all push worker exit success")
	case <-ctx.Done():
		return fmt.Errorf("shutdown wait worker timeout: %w", ctx.Err())
	}

	// 遍历关闭所有现存连接
	var closeCnt uint64
	m.conns.Range(func(k, v any) bool {
		//uid := k.(uint64)
		cli := castClientConn(v)
		if cli != nil {
			cli.Close()
			closeCnt++
		}
		m.conns.Delete(k)
		return true
	})
	// 统一清零在线计数
	m.onlineCount.Store(0)
	m.logger.Info(fmt.Sprintf("shutdown finish, closed conn count=%d", closeCnt))
	return nil
}

// ---------------- 监控指标获取接口 ----------------
func (m *ConnManager) GetOnlineNum() uint64 {
	return m.onlineCount.Load()
}

func (m *ConnManager) GetPushFailCount() uint64 {
	return m.pushFailCnt.Load()
}

func (m *ConnManager) GetReplaceCount() uint64 {
	return m.replaceCnt.Load()
}

func (m *ConnManager) GetDelCount() uint64 {
	return m.delCnt.Load()
}

// ---------------- 内部工具函数 ----------------
// castClientConn 统一类型转换，简化重复断言逻辑
func castClientConn(v any) ClientConnInter {
	cli, ok := v.(ClientConnInter)
	if !ok {
		return nil
	}
	return cli
}
