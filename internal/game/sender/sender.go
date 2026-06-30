package sender

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// PushSender 负责将业务事件批量发送给 pushserver
type PushSender struct {
	client    pb_service.PushServiceClient
	stream    pb_service.PushService_SendPushEventsClient
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	eventCh   chan *pb_service.PushEvent // 缓冲队列
	batchSize int
	ticker    *time.Ticker
	closed    bool
	mu        sync.Mutex
	clog      *zap.Logger
}

func NewPushSender(conn *grpc.ClientConn) (*PushSender, error) {
	client := pb_service.NewPushServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	// 创建客户端流
	stream, err := client.SendPushEvents(ctx)
	if err != nil {
		cancel()
		conn.Close()
		return nil, err
	}
	ps := &PushSender{
		client:    client,
		stream:    stream,
		ctx:       ctx,
		cancel:    cancel,
		eventCh:   make(chan *pb_service.PushEvent, 1000), // 超过1000 丢弃
		batchSize: 50,
		ticker:    time.NewTicker(100 * time.Millisecond),
		clog:      logger.L,
	}
	ps.wg.Add(1)
	go ps.worker()
	return ps, nil
}

// worker 从队列取出事件，批量发送
func (ps *PushSender) worker() {
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
				return
			}
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
		//return ErrQueueFull
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
	ps.wg.Wait()
	if err := ps.stream.CloseSend(); err != nil {
		// 忽略
		ps.clog.Warn("close_send failed", zap.Error(err))
	}
	ps.clog.Info("closed PushSender stream")
	return nil
}
