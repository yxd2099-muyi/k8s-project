package conn

import (
	"context"
	"runtime"
	"time"

	"github.com/gorilla/websocket"
	"github.com/k8s/muyi/api/db"
	"github.com/k8s/muyi/api/model"
	pb_base "github.com/k8s/muyi/api/pb/base"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

const (
	writeWait       = 10 * time.Second
	writeChanPushTO = 50 * time.Millisecond // 消息入队超时，防止阻塞
	waitGoroutineTO = 15 * time.Second      // 协程等待兜底超时
)

type ClientConn struct {
	ctx        context.Context
	cancel     context.CancelFunc
	uid        uint64
	gateAddr   string
	wsConn     *websocket.Conn
	writeChan  chan []byte // 读写分离写通道
	closeFlag  atomic.Bool
	redisCli   *rediscli.Client
	cfg        config.Gate
	userDb     *db.User
	clog       *zap.Logger
	tickerPing *time.Ticker
	wg         sync.WaitGroup
	mu         sync.Mutex
	connMgr    *ConnManager
}

func NewClientConn(ws *websocket.Conn, uid uint64, gateAddr string, redis *rediscli.Client, cfg config.Gate, connMgr *ConnManager) *ClientConn {
	ctx, cancel := context.WithCancel(context.Background())
	cli := &ClientConn{
		ctx:        ctx,
		cancel:     cancel,
		uid:        uid,
		gateAddr:   gateAddr,
		wsConn:     ws,
		writeChan:  make(chan []byte, 256),
		redisCli:   redis,
		cfg:        cfg,
		tickerPing: time.NewTicker(time.Duration(cfg.PingInterval) * time.Second),
		userDb:     db.NewUserObj(),
		clog:       logger.L,
		connMgr:    connMgr,
	}
	// 3个常驻协程
	cli.wg.Add(3)
	_ = cli.SetUserOnline(uid, gateAddr, cfg.RedisExpire)
	go cli.readLoop()
	go cli.writeLoop()
	go cli.syncUserOnlineLoop()
	return cli
}

// WriteMsg 外部下发消息，增加超时防止永久阻塞
func (c *ClientConn) WriteMsg(data []byte) error {
	if c.closeFlag.Load() {
		return websocket.ErrCloseSent
	}
	t := time.NewTimer(writeChanPushTO)
	defer t.Stop()

	select {
	case c.writeChan <- data:
		return nil
	case <-c.ctx.Done():
		return context.Canceled
	case <-t.C:
		c.clog.Warn("writeChan full drop msg",
			zap.Uint64("uid", c.uid),
			zap.Int("chan_len", len(c.writeChan)))
		return nil
	}
}

// readLoop 读协程
func (c *ClientConn) readLoop() {
	// 先捕获panic，再执行wg.Done，顺序不能颠倒
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			c.clog.Error("readLoop panic recover",
				zap.Uint64("uid", c.uid),
				zap.Any("panic", r),
				zap.String("stack", string(buf[:n])))
		}
		c.clog.Debug("readLoop exit")
		c.wg.Done()
		c.Close()
	}()

	pongTimeout := time.Duration(c.cfg.PongTimeout) * time.Second
	c.wsConn.SetPongHandler(func(string) error {
		_ = c.wsConn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	for {
		// 每次循环优先判断关闭信号
		select {
		case <-c.ctx.Done():
			c.clog.Debug("readLoop ctx cancel exit", zap.Uint64("uid", c.uid))
			return
		default:
		}

		_ = c.wsConn.SetReadDeadline(time.Now().Add(pongTimeout))
		msgType, data, err := c.wsConn.ReadMessage()
		if err != nil {
			c.clog.Info("ws read error trigger close", zap.Uint64("uid", c.uid), zap.Any("msgType", msgType), zap.Error(err))
			return
		}
		switch msgType {
		case websocket.BinaryMessage:
			wsFrame, err := serializer.DecodeWsFrame(data)
			if err != nil {
				c.clog.Warn("decode ws frame failed", zap.Uint64("uid", c.uid), zap.Error(err))
				continue
			}
			c.handleClientFrame(wsFrame)
		case websocket.CloseMessage:
			c.clog.Info("recv ws close frame", zap.Uint64("uid", c.uid))
			return
		}
	}
}

// writeLoop 写协程
func (c *ClientConn) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			c.clog.Error("writeLoop panic recover",
				zap.Uint64("uid", c.uid),
				zap.Any("panic", r),
				zap.String("stack", string(buf[:n])))
		}
		c.tickerPing.Stop()
		_ = c.wsConn.Close()
		c.wg.Done()
	}()

	for {
		select {
		case <-c.ctx.Done():
			c.clog.Debug("writeLoop ctx cancel exit", zap.Uint64("uid", c.uid))
			return
		case <-c.tickerPing.C:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.clog.Warn("send ping failed exit writeLoop", zap.Uint64("uid", c.uid), zap.Error(err))
				return
			}
		case msg := <-c.writeChan:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				c.clog.Warn("write binary msg failed exit writeLoop", zap.Uint64("uid", c.uid), zap.Error(err))
				return
			}
		}
	}
}

// syncUserOnlineLoop 定时刷新在线状态
func (c *ClientConn) syncUserOnlineLoop() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			c.clog.Error("syncUserOnlineLoop panic recover",
				zap.Uint64("uid", c.uid),
				zap.Any("panic", r),
				zap.String("stack", string(buf[:n])))
		}
		c.wg.Done()
	}()

	ticker := time.NewTicker(time.Duration(c.cfg.SyncInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			c.clog.Debug("syncUserOnlineLoop ctx cancel exit", zap.Uint64("uid", c.uid))
			return
		case <-ticker.C:
			_ = c.SetUserOnline(c.uid, c.gateAddr, c.cfg.RedisExpire)
		}
	}
}

func (c *ClientConn) SetUserOnline(uid uint64, gateAddr string, expireTime int) error {
	ctx, cancel := context.WithTimeout(context.Background(), cconst.ExpireTimeOut2s)
	defer cancel()
	session := &model.UserSession{
		UserID:      uid,
		GateAddress: gateAddr,
	}
	err := c.userDb.SetUserSession(ctx, c.clog, uid, session, time.Duration(expireTime)*time.Second)
	if err != nil {
		c.clog.Error("failed to set user online", zap.Uint64("uid", uid), zap.Error(err))
	}
	return err
}

func (c *ClientConn) DelUserSession(uid uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), cconst.ExpireTimeOut2s)
	defer cancel()
	c.userDb.DelUserSession(ctx, uid)
}

// Close 【修复核心】调整顺序+超时兜底，保证必打印finish日志
func (c *ClientConn) Close() {
	c.clog.Debug("client conn close start", zap.Uint64("uid", c.uid))
	if !c.closeFlag.CompareAndSwap(false, true) {
		return
	}

	// 1. 下发取消信号，所有协程循环感知退出
	c.cancel()
	// 2. 删除redis在线会话
	c.DelUserSession(c.uid)
	// 3. 强制关闭ws底层socket，打断阻塞读写IO
	_ = c.wsConn.Close()

	c.clog.Debug("client conn close wait goroutine", zap.Uint64("uid", c.uid))

	// 增加15s超时兜底，绝对不会永久阻塞wg.Wait
	waitCtx, waitCancel := context.WithTimeout(context.Background(), waitGoroutineTO)
	defer waitCancel()
	doneCh := make(chan struct{})

	go func() {
		c.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		c.clog.Debug("all goroutine normal exit")
	case <-waitCtx.Done():
		// 超时打印堆栈，定位卡死协程
		buf := make([]byte, 1024*1024)
		n := runtime.Stack(buf, true)
		c.clog.Error("wait goroutine timeout force exit",
			zap.Uint64("uid", c.uid),
			zap.String("full_stack", string(buf[:n])))
	}

	// 所有协程退出后再关闭writeChan，无协程读取，安全无panic
	close(c.writeChan)
	c.connMgr.DelConn(c.uid)
	// 这条日志100%会执行
	c.clog.Debug("client conn close finish all goroutine exit", zap.Uint64("uid", c.uid))
}

// 全局消息处理回调
var handleClientFrame func(frame *pb_base.WsFrame)

func RegisterFrameHandler(f func(frame *pb_base.WsFrame)) {
	handleClientFrame = f
}

func (c *ClientConn) handleClientFrame(frame *pb_base.WsFrame) {
	if handleClientFrame != nil {
		handleClientFrame(frame)
	}
}
