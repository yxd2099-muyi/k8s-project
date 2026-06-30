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

const (
	batchSize    = 50
	flushTimeout = 100 * time.Millisecond
	rpcTimeout   = 2 * time.Second // 单次RPC超时
	retryWait    = 200 * time.Millisecond
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

	gcli, err := grpcx.NewGrpcClient(gcfg, etcdCli.GetClient())
	if err != nil {
		clog.Error("InitPushSender create grpc client err", zap.Error(err))
		return nil, err
	}

	once.Do(func() {
		sender, err := NewPushSender(gcli)
		if err != nil {
			clog.Error("NewPushSender failed", zap.Error(err))
			return
		}
		GlobalPushSender = sender
	})

	clog.Info("InitPushSender end")
	return GlobalPushSender, nil
}

type PushSender struct {
	client       pb_service.PushServiceClient
	globalCtx    context.Context
	globalCancel context.CancelFunc
	wg           sync.WaitGroup
	eventCh      chan *pb_service.PushEvent
	ticker       *time.Ticker
	closed       bool
	mu           sync.Mutex
	clog         *zap.Logger
	grpcClient   *grpcx.GrpcClient
	failedBatch  []*pb_service.PushEvent // 发送失败暂存，重试不丢消息
}

func NewPushSender(grpcClient *grpcx.GrpcClient) (*PushSender, error) {
	client := pb_service.NewPushServiceClient(grpcClient.Conn())
	globalCtx, globalCancel := context.WithCancel(context.Background())

	ps := &PushSender{
		client:       client,
		globalCtx:    globalCtx,
		globalCancel: globalCancel,
		eventCh:      make(chan *pb_service.PushEvent, 1000),
		ticker:       time.NewTicker(flushTimeout),
		clog:         logger.L,
		grpcClient:   grpcClient,
	}

	ps.wg.Add(1)
	go ps.worker()
	return ps, nil
}

func (ps *PushSender) worker() {
	clog := ps.clog
	defer func() {
		if r := recover(); r != nil {
			clog.Error("worker panic recover", zap.Any("panic", r))
		}
		ps.wg.Done()
	}()

	batch := make([]*pb_service.PushEvent, 0, batchSize)

	sendBatch := func(list []*pb_service.PushEvent) bool {
		if len(list) == 0 {
			return true
		}

		// ✅ 每次RPC使用独立超时上下文，不再共用全局ctx
		ctx, cancel := context.WithTimeout(ps.globalCtx, rpcTimeout)
		defer cancel()

		stream, err := ps.client.SendPushEvents(ctx)
		if err != nil {
			clog.Error("create stream failed", zap.Error(err), zap.Int("count", len(list)))
			return false
		}

		for _, evt := range list {
			if evt.ExpireAt > 0 && evt.ExpireAt < time.Now().Unix() {
				continue
			}
			if err := stream.Send(evt); err != nil {
				clog.Error("stream send failed", zap.Error(err), zap.String("event_id", evt.EventId))
				return false
			}
		}

		resp, err := stream.CloseAndRecv()
		if err != nil {
			clog.Error("CloseAndRecv failed", zap.Error(err), zap.Int("count", len(list)))
			return false
		}

		clog.Debug("batch send success",
			zap.String("last_event_id", resp.EventId),
			zap.Bool("success", resp.Success),
			zap.Int("total", len(list)))
		return true
	}

	for {
		select {
		case <-ps.globalCtx.Done():
			// 退出前把剩余消息发送一次
			if len(batch) > 0 {
				_ = sendBatch(batch)
			}
			if len(ps.failedBatch) > 0 {
				_ = sendBatch(ps.failedBatch)
			}
			return

		case evt, ok := <-ps.eventCh:
			if !ok {
				if len(batch) > 0 {
					_ = sendBatch(batch)
				}
				return
			}
			batch = append(batch, evt)
			if len(batch) >= batchSize {
				if !sendBatch(batch) {
					// 发送失败，存入失败队列等待下一轮重试
					ps.failedBatch = append(ps.failedBatch, batch...)
				}
				batch = batch[:0]
			}

		case <-ps.ticker.C:
			// 先重试上一轮失败的批次
			if len(ps.failedBatch) > 0 {
				if sendBatch(ps.failedBatch) {
					ps.failedBatch = ps.failedBatch[:0]
				} else {
					time.Sleep(retryWait)
				}
			}
			// 再发送当前积攒批次
			if len(batch) > 0 {
				if !sendBatch(batch) {
					ps.failedBatch = append(ps.failedBatch, batch...)
				}
				batch = batch[:0]
			}
		}
	}
}

// Push 非阻塞入队
func (ps *PushSender) Push(evt *pb_service.PushEvent) error {
	ps.mu.Lock()
	closed := ps.closed
	ps.mu.Unlock()
	if closed {
		ps.clog.Warn("sender already closed, drop event", zap.String("event_id", evt.EventId))
		return nil
	}

	select {
	case ps.eventCh <- evt:
		return nil
	default:
		ps.clog.Warn("push queue full drop event", zap.String("event_id", evt.EventId))
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

	ps.globalCancel()
	ps.ticker.Stop()
	close(ps.eventCh)
	ps.wg.Wait()
	ps.grpcClient.Close()

	ps.clog.Info("PushSender graceful closed")
	return nil
}

// GetPushEvent 组装推送消息
func GetPushEvent(sendUid uint64, uids []uint64, cmd pb_push.CmdPushKind, p proto.Message) *pb_service.PushEvent {
	one := &pb_service.PushEvent{}
	one.EventId = kit.NewShortUUID()
	one.SendUid = sendUid
	one.Uids = uids
	ts := time.Now().Unix()
	one.Timestamp = ts
	one.ExpireAt = ts + 24*60*60*120

	payload, _ := serializer.EncodeProto(p)
	req := &pb_base.PushBody{
		Cmd:     uint32(cmd),
		Payload: payload,
	}
	payload, _ = serializer.EncodeProto(req)
	one.Payload = payload
	return one
}

func PushEvent(evt *pb_service.PushEvent) {
	if GlobalPushSender != nil {
		_ = GlobalPushSender.Push(evt)
	}
}
