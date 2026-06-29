package pushclient

import (
	"context"
	"fmt"
	pb "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"io"

	//pb_service "github.com/k8s/muyi/api/pb/service"
	"google.golang.org/grpc"
	"runtime/debug"
	"sync"
	"time"
)

type PushEvent struct {
	//*pb_service.PushEvent
	*pb.PushEvent
}

func generateUUID() string {
	return "" //todo
}
func NewPushEvent(sendUID uint64, uids []uint64, payload []byte) *PushEvent {
	return &PushEvent{
		PushEvent: &pb.PushEvent{
			EventId:   generateUUID(),
			SendUid:   sendUID,
			Uids:      uids,
			Payload:   payload,
			Timestamp: time.Now().Unix(),
			ExpireAt:  time.Now().Add(30 * time.Minute).Unix(),
		},
	}
}

type Client struct {
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	queue      chan *PushEvent
	wg         sync.WaitGroup
	conn       *grpc.ClientConn
	grpcClient pb.PushServiceClient
	clog       *zap.Logger
}

func NewClient(conn *grpc.ClientConn) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		ctx:        ctx,
		cancel:     cancel,
		queue:      make(chan *PushEvent, 30000),
		conn:       conn,
		grpcClient: pb.NewPushServiceClient(conn),
		clog:       logger.L,
	}

	c.startWorkers(6) // 推荐多个 worker
	return c, nil
}

// 异步批量发送
func (c *Client) SendAsync(events ...*PushEvent) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.ctx.Err() != nil {
		return
	}

	for _, e := range events {
		select {
		case c.queue <- e:
		case <-c.ctx.Done():
			return
		default:
			c.clog.Warn("[PushClient] queue full")
		}
	}
}

func (c *Client) startWorkers(num int) {
	for i := 0; i < num; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}
}

// 多个 worker 优势：高并发、避免阻塞、更好负载
func (c *Client) worker(id int) {
	clog := c.clog
	defer c.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			clog.Error(fmt.Sprintf("[PushClient Worker %d] panic recovered: %v\n%s", id, r, debug.Stack()))
		}
	}()

	stream, err := c.grpcClient.SendPushEvents(c.ctx)
	if err != nil {
		clog.Error(fmt.Sprintf("worker %d: stream create failed: %v", id, err))
		return
	}
	// 用于优雅退出时关闭流
	defer func() {
		if _, closeErr := stream.CloseAndRecv(); closeErr != nil && closeErr != io.EOF {
			clog.Error(fmt.Sprintf("worker %d: CloseAndRecv error: %v", id, closeErr))
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-c.queue:
			if !ok {
				return
			}
			if err := stream.Send(event.PushEvent); err != nil {
				clog.Error(fmt.Sprintf("worker %d: send failed: %v", id, err))
			}
		}
	}
}

func (c *Client) Close() {
	c.cancel()     // 通知所有 worker 退出
	close(c.queue) // 防止 goroutine 卡住
	c.wg.Wait()    // 等待所有 worker 完成
	if c.conn != nil {
		c.conn.Close()
	}
}
