package sender

import (
	"context"
	pb_base "github.com/k8s/muyi/api/pb/base"
	pb_push "github.com/k8s/muyi/api/pb/push"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/etcdx"
	"github.com/k8s/muyi/shared/infra/grpcx"
	"github.com/k8s/muyi/shared/infra/logger"
	"github.com/k8s/muyi/shared/kit"
	"github.com/k8s/muyi/shared/kit/serializer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

var (
	GlobalPushSender *PushSender
	once             sync.Once
)

func InitPushSender() (*PushSender, error) {
	gcfg := grpcx.DefaultClientConfig()
	target := etcdx.GetEtcdPushServerTarget()
	gcfg.Target = target
	gcfg.TargetType = grpcx.TargetTypeEtcd
	gcfg.LBPolicy = string(cconst.LBRoundRobin)
	etcdCli := etcdx.GetGlobalLeaseEtcd()
	clog := logger.L
	clog.Debug("InitPushSender start")
	var err error
	gcli, err := grpcx.NewGrpcClient(gcfg, etcdCli.GetClient())
	if err != nil {
		clog.Error("InitPushSender start", zap.Error(err))
		return nil, err
	}
	once.Do(func() {
		sender, err := NewPushSender(gcli)
		if err != nil {
			clog.Error("InitPushSender start", zap.Error(err))
			return
		}
		GlobalPushSender = sender
	})

	clog.Info("InitPushSender end")
	return GlobalPushSender, nil
}

// PushSender 负责将业务事件批量发送给 pushserver
type PushSender struct {
	client     pb_service.PushServiceClient
	stream     pb_service.PushService_SendPushEventsClient
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	eventCh    chan *pb_service.PushEvent // 缓冲队列
	batchSize  int
	ticker     *time.Ticker
	closed     bool
	mu         sync.Mutex
	clog       *zap.Logger
	grpcClient *grpcx.GrpcClient
}

func NewPushSender(grpcClient *grpcx.GrpcClient) (*PushSender, error) {

	client := pb_service.NewPushServiceClient(grpcClient.Conn())
	ctx, cancel := context.WithCancel(context.Background())
	// 创建客户端流
	stream, err := client.SendPushEvents(ctx)
	if err != nil {
		cancel()
		grpcClient.Close()
		return nil, err
	}
	ps := &PushSender{
		client:     client,
		stream:     stream,
		ctx:        ctx,
		cancel:     cancel,
		eventCh:    make(chan *pb_service.PushEvent, 1000), // 超过1000 丢弃
		batchSize:  50,
		ticker:     time.NewTicker(100 * time.Millisecond),
		clog:       logger.L,
		grpcClient: grpcClient,
	}
	clog := ps.clog
	clog.Debug("InitPushSender start")
	ps.wg.Add(1)
	go ps.worker()
	return ps, nil
}

// worker 从队列取出事件，批量发送
func (ps *PushSender) worker() {
	clog := ps.clog
	defer func() {
		if r := recover(); r != nil {
			// log panic
			ps.clog.Error("recover from panic", zap.Any("recover", r))
		}
		ps.wg.Done()
	}()
	batch := make([]*pb_service.PushEvent, 0, ps.batchSize)
	sendBatch := func() {
		if len(batch) == 0 {
			return
		}
		// 循环发送每个事件
		for _, evt := range batch {
			select {
			case <-ps.ctx.Done():
				return
			default:
			}
			// 过期检查
			if evt.ExpireAt > 0 && evt.ExpireAt < time.Now().Unix() {
				continue
			}
			if err := ps.stream.Send(evt); err != nil {
				// 记录错误，可能重试，但这里仅打印
				// 注意：如果流关闭，需要退出
				clog.Error("push send failed", zap.Error(err))
				return
			}
			clog.Debug("send event success", zap.Any("event", evt))
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ps.ctx.Done():
			sendBatch()
			return
		case evt, ok := <-ps.eventCh:
			if !ok {
				sendBatch()
				return
			}
			batch = append(batch, evt)
			if len(batch) >= ps.batchSize {
				sendBatch()
			}
		case <-ps.ticker.C:
			sendBatch()
		}
	}
}

// Push 将事件放入队列（非阻塞）
func (ps *PushSender) Push(evt *pb_service.PushEvent) error {
	select {
	case ps.eventCh <- evt:
		return nil
	default:
		ps.clog.Warn("push event failed QueueFull", zap.Any("evt", evt))
		//return ErrQueueFully
		return nil
	}
}

// Close 优雅关闭
func (ps *PushSender) Close() error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil
	}
	ps.closed = true
	ps.mu.Unlock()
	ps.cancel()
	ps.ticker.Stop()
	close(ps.eventCh)
	ps.grpcClient.Close()
	ps.wg.Wait()
	if err := ps.stream.CloseSend(); err != nil {
		// 忽略
		ps.clog.Warn("close_send failed", zap.Error(err))
	}
	ps.clog.Info("closed PushSender stream")
	return nil
}

// 获取推送对象
func GetPushEvent(sendUid uint64, uids []uint64, cmd pb_push.CmdPushKind, p proto.Message) *pb_service.PushEvent {
	one := &pb_service.PushEvent{}
	one.EventId = kit.NewShortUUID()
	one.SendUid = sendUid
	one.Uids = uids
	ts := time.Now().Unix()
	one.Timestamp = ts
	one.ExpireAt = ts + 24*60*60*120

	payload, _ := serializer.EncodeProto(p)
	req := &pb_base.PushBody{}
	req.Cmd = uint32(cmd)
	req.Payload = payload
	payload, _ = serializer.EncodeProto(req)
	one.Payload = payload
	return one
}
func PushEvent(evt *pb_service.PushEvent) {
	GlobalPushSender.Push(evt)
}
