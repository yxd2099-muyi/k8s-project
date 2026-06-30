package grpc_server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
)

type GateConn struct {
	stream  pb_service.PushService_PushStreamServer
	router  *Router
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	sendCh  chan *pb_service.PushToGate
	address string // 用于标识，可从 metadata 获取
	clog    *zap.Logger
}

func NewGateConn(stream pb_service.PushService_PushStreamServer, router *Router, parentCtx context.Context) *GateConn {
	ctx, cancel := context.WithCancel(parentCtx)
	return &GateConn{
		stream: stream,
		router: router,
		ctx:    ctx,
		cancel: cancel,
		sendCh: make(chan *pb_service.PushToGate, 100),
		clog:   logger.L,
		// address 可从 stream.Context() 中提取，或由 gate 发送消息时附带
	}
}

func (gc *GateConn) Handle() error {
	clog := gc.clog
	// 启动发送 goroutine
	gc.wg.Add(1)
	go gc.sendLoop()
	// 主循环接收来自 gate 的消息
	defer func() {
		// 清理路由
		gc.router.RemoveGate(gc)
		gc.cancel()
		gc.wg.Wait()
		close(gc.sendCh)
	}()
	for {
		req, err := gc.stream.Recv()
		if err != nil {
			clog.Error("Recv", zap.Error(err))
			return err
		}
		switch payload := req.Payload.(type) {
		case *pb_service.GateToPush_Online:
			// 更新路由
			gc.router.Add(payload.Online.UserId, gc)
			// 可以记录 gateAddress
		case *pb_service.GateToPush_Offline:
			gc.router.Remove(payload.Offline.UserId)
		case *pb_service.GateToPush_Ack:
			// 可选处理确认
		}
	}
}

// sendLoop 从 sendCh 取消息发送
func (gc *GateConn) sendLoop() {
	clog := gc.clog
	defer func() {
		if r := recover(); r != nil {
			// log
			clog.Error("panic", zap.Any("recover", r))
		}
		gc.wg.Done()
	}()
	for {
		select {
		case <-gc.ctx.Done():
			return
		case msg, ok := <-gc.sendCh:
			if !ok {
				clog.Info("send channel closed")
				return
			}
			if err := gc.stream.Send(msg); err != nil {
				// 发送失败，可能连接断开
				return
			}
		}
	}
}

// SendPush 向该 gate 推送消息（非阻塞）
func (gc *GateConn) SendPush(msg *pb_service.PushToGate) {
	select {
	case gc.sendCh <- msg:
	case <-gc.ctx.Done():
	default:
		// 通道满，记录丢弃
		gc.clog.Warn("send channel full")
	}
}
