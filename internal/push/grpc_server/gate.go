package grpc_server

import (
	"context"
	"sync/atomic"

	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GateConn struct {
	stream pb_service.PushService_PushStreamServer
	router *Router
	ctx    context.Context
	cancel context.CancelFunc

	sendCh chan *pb_service.PushToGate

	gateAddr string
	clog     *zap.Logger
	closed   int32
}

func NewGateConn(stream pb_service.PushService_PushStreamServer, router *Router) *GateConn {
	ctx, cancel := context.WithCancel(stream.Context())

	gateAddr := "unknown"
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if arr := md.Get(cconst.ContextFieldGateAddress); len(arr) > 0 {
			gateAddr = arr[0]
		}
	}

	clog := logger.L.With(zap.String("gate_addr", gateAddr))

	return &GateConn{
		stream:   stream,
		router:   router,
		ctx:      ctx,
		cancel:   cancel,
		sendCh:   make(chan *pb_service.PushToGate, 512),
		gateAddr: gateAddr,
		clog:     clog,
	}
}

func (gc *GateConn) Handle() error {
	gc.clog.Info("gate conn established")

	// 启动发送循环
	go gc.sendLoop()

	// 接收循环（主协程）
	recvErr := gc.recvLoop()

	// 清理
	gc.doClose()

	if st, ok := status.FromError(recvErr); ok && st.Code() == codes.Canceled {
		gc.clog.Info("gate stream closed normally")
		return nil
	}
	if recvErr != nil {
		gc.clog.Warn("gate stream closed abnormally", zap.Error(recvErr))
	}
	return recvErr
}

func (gc *GateConn) doClose() {
	if !atomic.CompareAndSwapInt32(&gc.closed, 0, 1) {
		return
	}

	gc.clog.Info("gate conn closing...")
	gc.cancel()      // 通知 sendLoop 退出
	close(gc.sendCh) // 让 sendLoop 退出 range
	gc.router.RemoveGate(gc)
	gc.clog.Info("gate conn closed")
}

// recvLoop - 直接 Recv，不再额外 goroutine
func (gc *GateConn) recvLoop() error {
	for {
		select {
		case <-gc.ctx.Done():
			return context.Canceled
		default:
		}

		req, err := gc.stream.Recv()
		if err != nil {
			gc.clog.Error("gate conn recv error", zap.Error(err))
			return err
		}

		switch p := req.Payload.(type) {
		case *pb_service.GateToPush_Online:
			gc.router.Add(p.Online.UserId, gc)
		case *pb_service.GateToPush_Offline:
			gc.router.Remove(p.Offline.UserId)
		case *pb_service.GateToPush_Ack:
			gc.clog.Debug("recv ack", zap.String("event_id", p.Ack.EventId))
		}
	}
}

func (gc *GateConn) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			gc.clog.Error("sendLoop panic", zap.Any("r", r))
		}
	}()

	for msg := range gc.sendCh {
		if err := gc.stream.Send(msg); err != nil {
			gc.clog.Error("stream.Send failed", zap.Error(err))
			return
		}
	}
}

func (gc *GateConn) SendPush(msg *pb_service.PushToGate) {
	if atomic.LoadInt32(&gc.closed) == 1 {
		return
	}
	select {
	case gc.sendCh <- msg:
	case <-gc.ctx.Done():
	default:
		gc.clog.Warn("sendCh full, drop", zap.String("event_id", msg.EventId))
	}
}

// 外部强制关闭（Shutdown 时调用）
func (gc *GateConn) Close() {
	gc.doClose()
}
