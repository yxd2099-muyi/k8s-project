package grpc_server

import (
	"context"
	"github.com/k8s/muyi/shared/infra/cconst"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
)

// GateConn 一条Gate与Push之间的双向长连接
// 收发分离：单协程接收上行消息，单协程下发推送消息
// gRPC双向流Send方法非并发安全，保证sendLoop为唯一发送协程
type GateConn struct {
	stream pb_service.PushService_PushStreamServer
	router *Router
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	sendCh chan *pb_service.PushToGate // 下发消息队列，缓冲防阻塞

	gateAddr string // 网关唯一标识，从请求头metadata读取
	clog     *zap.Logger
}

func NewGateConn(stream pb_service.PushService_PushStreamServer, router *Router, parentCtx context.Context) *GateConn {
	ctx, cancel := context.WithCancel(parentCtx)

	// 提取gate地址 metadata: gate-addr=ip:port
	gateAddr := "unknown"
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok {
		arr := md.Get(cconst.ContextFieldGateAddress)
		if len(arr) > 0 {
			gateAddr = arr[0]
		}
	}
	clog := logger.L.With(zap.String("gate_addr", gateAddr))
	clog.Info("gate conn", zap.String("gate_addr", gateAddr))
	return &GateConn{
		stream:   stream,
		router:   router,
		ctx:      ctx,
		cancel:   cancel,
		sendCh:   make(chan *pb_service.PushToGate, 100),
		gateAddr: gateAddr,
		clog:     clog,
	}
}

func (gc *GateConn) Handle() error {
	clog := gc.clog

	// 启动发送协程
	gc.wg.Add(1)
	go gc.sendLoop()

	// 接收上行消息
	recvErr := gc.recvLoop()

	// ========== 优雅关闭严格时序 ==========
	clog.Info("gate stream begin shutdown", zap.Error(recvErr))

	// 1. 切断所有协程上下文
	gc.cancel()
	// 2. 等待发送协程把队列剩余消息发送完毕
	gc.wg.Wait()
	// 3. 网关断开，批量清理该网关下所有玩家路由（防止脏路由残留）
	gc.router.RemoveGate(gc)
	// 4. 关闭通道
	close(gc.sendCh)

	// 区分正常关闭与异常错误，屏蔽context取消的冗余ERROR日志
	if st, ok := status.FromError(recvErr); ok && st.Code() == codes.Canceled {
		clog.Info("gate stream closed normally (context canceled)")
		return nil
	}
	if recvErr != nil {
		clog.Warn("gate stream closed abnormally", zap.Error(recvErr))
	}
	return recvErr
}

// recvLoop 循环接收Gate上报：上线、下线、ACK回执
func (gc *GateConn) recvLoop() error {
	for {
		select {
		case <-gc.ctx.Done():
			return context.Canceled
		default:
		}

		req, err := gc.stream.Recv()
		if err != nil {
			return err
		}

		switch payload := req.Payload.(type) {
		case *pb_service.GateToPush_Online:
			gc.router.Add(payload.Online.UserId, gc)
		case *pb_service.GateToPush_Offline:
			gc.router.Remove(payload.Offline.UserId)
		case *pb_service.GateToPush_Ack:
			gc.clog.Debug("receive push ack",
				zap.String("event_id", payload.Ack.EventId),
				zap.Uint64("uid", payload.Ack.UserId))
		}
	}
}

// sendLoop 唯一发送协程，保证stream.Send串行调用
func (gc *GateConn) sendLoop() {
	clog := gc.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("sendLoop panic recovered", zap.Any("panic", r))
		}
		gc.wg.Done()
	}()

	for {
		select {
		case <-gc.ctx.Done():
			// 退出前排空队列剩余消息，尽量避免消息丢失
			gc.flushRemainingMsg()
			return

		case msg, ok := <-gc.sendCh:
			if !ok {
				return
			}
			if err := gc.stream.Send(msg); err != nil {
				clog.Error("send push message failed",
					zap.Error(err),
					zap.String("event_id", msg.EventId))
				return
			}
		}
	}
}

// flushRemainingMsg 上下文关闭时，尽力发送残留消息
func (gc *GateConn) flushRemainingMsg() {
	for {
		select {
		case msg, ok := <-gc.sendCh:
			if !ok {
				return
			}
			_ = gc.stream.Send(msg)
		default:
			return
		}
	}
}

// SendPush 非阻塞写入推送队列
func (gc *GateConn) SendPush(msg *pb_service.PushToGate) {
	select {
	case gc.sendCh <- msg:
		return
	case <-gc.ctx.Done():
		// 连接已关闭，静默丢弃，不打告警
		return
	default:
		gc.clog.Warn("send channel full, drop push message",
			zap.String("event_id", msg.EventId))
	}
}
