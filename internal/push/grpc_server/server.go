package grpc_server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"sync"
	"time"
)

type PushServer struct {
	pb_service.UnimplementedPushServiceServer
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	router    *Router                    // uid → gateConn
	eventCh   chan *pb_service.PushEvent // 推送事件队列
	workerNum int
	clog      *zap.Logger
}

func NewPushServer(workerNum int) *PushServer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &PushServer{
		ctx:       ctx,
		cancel:    cancel,
		router:    NewRouter(),
		eventCh:   make(chan *pb_service.PushEvent, 10000),
		workerNum: workerNum,
		clog:      logger.L,
	}
	// 启动推送 worker 池
	for i := 0; i < workerNum; i++ {
		s.wg.Add(1)
		go s.pushWorker()
	}
	return s
}

// SendPushEvents 处理 gameserver 的客户端流
func (s *PushServer) SendPushEvents(stream pb_service.PushService_SendPushEventsServer) error {
	clog := s.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("panic", zap.Any("recover", r))
		}
	}()
	for {
		evt, err := stream.Recv()
		if err != nil {
			clog.Error("recv error", zap.Error(err))
			return err
		}
		// 检查过期
		if evt.ExpireAt > 0 && evt.ExpireAt < time.Now().Unix() {
			continue
		}
		select {
		case s.eventCh <- evt:
			// 入队成功
		case <-s.ctx.Done():
			clog.Info("context done")
			return s.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			// 队列满，记录日志并丢弃
			clog.Warn("push events full")
		}
	}
}

// pushWorker 从队列取事件并推送到对应 gate
func (s *PushServer) pushWorker() {
	clog := s.clog
	defer func() {
		if r := recover(); r != nil {
			// log
			clog.Error("panic", zap.Any("recover", r))
		}
		s.wg.Done()
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case evt, ok := <-s.eventCh:
			if !ok {
				clog.Info("push events channel closed")
				return
			}
			// 按 gate 分组
			gateMap := make(map[*GateConn][]uint64)
			for _, uid := range evt.Uids {
				gc := s.router.Get(uid)
				if gc == nil {
					// 用户不在线或未上报路由
					continue
				}
				gateMap[gc] = append(gateMap[gc], uid)
			}
			// 向每个 gate 发送
			for gc, uids := range gateMap {
				pushMsg := &pb_service.PushToGate{
					EventId: evt.EventId,
					Uids:    uids,
					Payload: evt.Payload,
					// msg_type 可根据业务扩展
				}
				// 异步发送（非阻塞）
				gc.SendPush(pushMsg)
			}
		}
	}
}

// PushStream 处理 gateserver 的双向流
func (s *PushServer) PushStream(stream pb_service.PushService_PushStreamServer) error {
	defer func() {
		if r := recover(); r != nil {
			// log
		}
	}()
	// 创建 gateConn
	gc := NewGateConn(stream, s.router, s.ctx)
	// 注册到 router（可能只需要记录 gate 对应的 uids）
	// 启动接收和发送 goroutine
	return gc.Handle()
}

// Close 优雅关闭
func (s *PushServer) Close() {
	s.cancel()
	close(s.eventCh)
	s.wg.Wait()
	s.router.Close()
}
