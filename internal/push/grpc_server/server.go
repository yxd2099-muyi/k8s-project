package grpc_server

import (
	"context"
	"errors"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"io"
	"runtime/debug"
	"sync"
	"time"
)

type PushServer struct {
	pb_service.UnimplementedPushServiceServer
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	router    *Router
	eventCh   chan *pb_service.PushEvent
	workerNum int
	clog      *zap.Logger
	gateConns []*GateConn
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
	for i := 0; i < workerNum; i++ {
		s.wg.Add(1)
		go s.pushWorker()
	}
	return s
}

// SendPushEvents 接收GameServer客户端流
func (s *PushServer) SendPushEvents(stream pb_service.PushService_SendPushEventsServer) error {
	clog := s.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("SendPushEvents panic", zap.Any("recover", r))
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		evt, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				resp := &pb_service.PushResponse{
					Success: true,
					Msg:     "ok",
				}
				if errSend := stream.SendAndClose(resp); errSend != nil {
					clog.Warn("SendAndClose failed", zap.Error(errSend))
				}
				return nil
			}
			clog.Error("stream recv error", zap.Error(err))
			return err
		}

		if evt.ExpireAt > 0 && evt.ExpireAt < time.Now().Unix() {
			clog.Debug("push event expired drop", zap.String("event_id", evt.EventId))
			continue
		}

		select {
		case s.eventCh <- evt:
			clog.Debug("recv push event", zap.String("event_id", evt.EventId))
		case <-s.ctx.Done():
			clog.Info("server context cancel, exit stream")
			return s.ctx.Err()
		case <-stream.Context().Done():
			clog.Info("client stream context cancel")
			return stream.Context().Err()
		default:
			clog.Warn("eventCh full drop push event", zap.String("event_id", evt.EventId))
		}
	}
}

// pushWorker 分发推送消息到Gate
func (s *PushServer) pushWorker() {
	clog := s.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("pushWorker panic", zap.Any("recover", r))
		}
		s.wg.Done()
	}()

	gateMap := make(map[*GateConn][]uint64)
	for {
		select {
		case <-s.ctx.Done():
			return
		case evt, ok := <-s.eventCh:
			if !ok {
				clog.Info("eventCh closed, pushWorker exit")
				return
			}

			for k := range gateMap {
				delete(gateMap, k)
			}

			for _, uid := range evt.Uids {
				gc := s.router.Get(uid)
				if gc == nil {
					continue
				}
				gateMap[gc] = append(gateMap[gc], uid)
			}

			for gc, uids := range gateMap {
				msg := &pb_service.PushToGate{
					EventId: evt.EventId,
					Uids:    uids,
					Payload: evt.Payload,
				}
				gc.SendPush(msg)
			}
		}
	}
}

// 【核心修复】父上下文改为 stream.Context()，不再依赖服务全局ctx
func (s *PushServer) PushStream(stream pb_service.PushService_PushStreamServer) error {
	defer func() {
		if r := recover(); r != nil {
			s.clog.Error("PushStream panic",
				zap.Any("recover", r),
				zap.String("stack", string(debug.Stack())))
		}
	}()
	// 旧：s.ctx
	// 新：使用流自带上下文，gRPC关闭流自动触发取消
	gc := NewGateConn(stream, s.router)

	//s.gateConns = append(s.gateConns, gc)
	err := gc.Handle()
	if err != nil {
		s.clog.Error("PushStream Handle", zap.Error(err))
	}
	return err
}

func (s *PushServer) Close() {
	s.clog.Info("PushServer close start")
	s.cancel()
	close(s.eventCh)
	s.wg.Wait()
	s.router.Close()
	//for _, gc := range s.gateConns {
	//	gc.Close()
	//}
	s.clog.Info("pushserver graceful closed")
}
