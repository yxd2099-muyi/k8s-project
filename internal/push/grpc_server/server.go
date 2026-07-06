package grpc_server

import (
	"context"
	"errors"
	rmq "github.com/apache/rocketmq-clients/golang/v5"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/infra/mq"
	"github.com/k8s/muyi/shared/kit/serializer"
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
	//mqConsumer *mq.Consumer // 这个地方可以是一个接口 TODO
	//mqConsumer *mq.PushConsumer // 这个地方可以是一个接口 TODO
	mqConsumer mq.IMQConsumer // 这个地方可以是一个接口 TODO
}

func NewPushServer(workerNum int) (*PushServer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	clog := logger.L
	s := &PushServer{
		ctx:       ctx,
		cancel:    cancel,
		router:    NewRouter(),
		eventCh:   make(chan *pb_service.PushEvent, 10000),
		workerNum: workerNum,
		clog:      clog,
	}
	consumer, err := mq.NewConsumer(cconst.ConsumerGroupChat)
	//consumer, err := mq.NewPushConsumer(cconst.ConsumerGroupChat)
	if err != nil {
		clog.Error("NewConsumer error", zap.Error(err))
		return nil, err
	}
	consumer.RegisterHandler(cconst.TopicPushEvents, cconst.TagPushEventChat, s.HandlerPushMQ) //todo 注册这个可以做成全局的， 这里循环读 别处init 做， 这里循环读
	err = consumer.Start()
	if err != nil {
		clog.Error("Start error", zap.Error(err))
		return nil, err
	}

	s.mqConsumer = consumer
	for i := 0; i < workerNum; i++ {
		s.wg.Add(1)
		go s.pushWorker()
	}

	return s, nil
}

// 处理MQ 中的消息
func (s *PushServer) HandlerPushMQ(ctx context.Context, msg *rmq.MessageView) error {
	clog := s.clog
	defer func() {
		if err := recover(); err != nil {
			clog.Error("Handler PushMQ panic", zap.Any("err", err))
			stack := string(debug.Stack())
			clog.Error("handler panic recover",
				zap.Any("panic_reason", err),
				zap.String("stack", stack),
			)
		}
	}()

	body := msg.GetBody()
	if len(body) == 0 {
		return nil
	}
	var event pb_service.PushEvent
	err := serializer.DecodeProto(body, &event)
	if err != nil {
		clog.Error("DecodeProto error", zap.Error(err))
		return err
	}
	evt := &event
	clog.Debug("push msg end", zap.Any("evt", evt))
	select {
	case s.eventCh <- evt:
		clog.Debug("HandlerPushMQ recv push event", zap.String("event_id", evt.EventId))
	case <-s.ctx.Done():
		clog.Info("HandlerPushMQ server context cancel, exit stream")
		return s.ctx.Err()
	default:
		clog.Warn(" HandlerPushMQ eventCh full drop push event", zap.String("event_id", evt.EventId))
	}
	return nil
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
	s.mqConsumer.GracefulStop()
	s.wg.Wait()
	s.router.Close()
	//for _, gc := range s.gateConns {
	//	gc.Close()
	//}
	s.clog.Info("pushserver graceful closed")
}
