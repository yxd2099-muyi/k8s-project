package client_conn

import (
	"context"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/k8s/muyi/shared/infra/redisClient"
	"github.com/k8s/muyi/shared/kit/serializer"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	pb_base "github.com/k8s/muyi/api/pb/base"
)

const (
	writeWait = 10 * time.Second
)

type ClientConn struct {
	ctx        context.Context
	cancel     context.CancelFunc
	uid        uint64
	gateAddr   string
	wsConn     *websocket.Conn
	writeChan  chan []byte // 写协程通道，读写分离
	closeFlag  atomic.Bool
	redisCli   *redisClient.Client
	cfg        config.Gate
	tickerPing *time.Ticker
	wg         sync.WaitGroup
	mu         sync.Mutex
}

func NewClientConn(ws *websocket.Conn, uid uint64, gateAddr string, redis *redisClient.Client, cfg config.Gate) *ClientConn {
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
	}
	cli.wg.Add(2)
	// 读写协程分离
	go cli.readLoop()
	go cli.writeLoop()
	// 定时同步用户信息到redis
	go cli.syncUserOnlineLoop()
	return cli
}

// WriteMsg 外部写入消息给客户端
func (c *ClientConn) WriteMsg(data []byte) error {
	if c.closeFlag.Load() {
		return websocket.ErrCloseSent
	}
	select {
	case c.writeChan <- data:
		return nil
	case <-c.ctx.Done():
		return context.Canceled
	}
}

// readLoop 读协程：接收客户端消息、处理ping/pong
func (c *ClientConn) readLoop() {
	defer func() {
		c.Close()
		c.wg.Done()
	}()
	_ = c.wsConn.SetReadDeadline(time.Now().Add(time.Duration(c.cfg.PongTimeout) * time.Second))
	c.wsConn.SetPongHandler(func(string) error {
		_ = c.wsConn.SetReadDeadline(time.Now().Add(time.Duration(c.cfg.PongTimeout) * time.Second))
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msgType, data, err := c.wsConn.ReadMessage()
			if err != nil {
				return
			}
			switch msgType {
			case websocket.BinaryMessage:
				// 上层hub/网关逻辑处理WsFrame
				wsFrame, err := serializer.DecodeWsFrame(data)
				if err != nil {
					continue
				}
				// 投递到网关主处理逻辑（gate服务层接收）
				c.handleClientFrame(wsFrame)
			case websocket.CloseMessage:
				return
			}
		}
	}
}

// writeLoop 写协程：统一发包、定时ping探活
func (c *ClientConn) writeLoop() {
	defer func() {
		c.tickerPing.Stop()
		_ = c.wsConn.Close()
		c.wg.Done()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.tickerPing.C:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case msg := <-c.writeChan:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		}
	}
}

// syncUserOnlineLoop 定时刷新redis用户在线信息，保活过期
func (c *ClientConn) syncUserOnlineLoop() {
	ticker := time.NewTicker(time.Duration(c.cfg.SyncInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// todo
			_ = c.SetUserOnline(c.uid, c.gateAddr, c.cfg.RedisExpire)
		}
	}
}
func (c *ClientConn) SetUserOnline(uid uint64, gateAddr string, expireTime int) error {
	//todo redis 上传个人信息
	return nil
}

// Close 安全关闭连接
func (c *ClientConn) Close() {
	if c.closeFlag.Swap(true) {
		return
	}
	c.cancel()
	_ = c.wsConn.Close()
	close(c.writeChan)
	c.wg.Wait()
}

// handleClientFrame 外部注入：交给gate服务处理ws帧
var handleClientFrame func(frame *pb_base.WsFrame)

func RegisterFrameHandler(f func(frame *pb_base.WsFrame)) {
	handleClientFrame = f
}

//	func (c *ClientConn) RegisterFrameHandler(f func(frame *pb_base.WsFrame)) {
//		handleClientFrame = f
//	}
func (c *ClientConn) handleClientFrame(frame *pb_base.WsFrame) {
	if handleClientFrame != nil {
		handleClientFrame(frame)
	}
}
