package grpc_server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/common"
	"github.com/k8s/muyi/shared/infra/cconst"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"sync"
)

type PushClient struct {
	conn   *grpc.ClientConn
	stream pb_service.PushService_PushStreamClient

	// 统一上下文
	ctx    context.Context
	cancel context.CancelFunc

	wg      sync.WaitGroup
	sendCh  chan *pb_service.GateToPush
	recvCh  chan *pb_service.PushToGate
	gateSrv *PushServer
	clog    *zap.Logger
}

func NewPushClient(conn *grpc.ClientConn, gs *PushServer, argCfg *common.ArgConf) (*PushClient, error) {
	clog := gs.clog
	// 1. 先创建可取消的根上下文，不要中途覆盖cancel句柄
	baseCtx, baseCancel := context.WithCancel(context.Background())

	// 2. 把metadata绑定到baseCtx，不要新建空白context，防止上下文断裂
	gateAddr := argCfg.GRpcAddressRegister
	md := metadata.Pairs(cconst.ContextFieldGateAddress, gateAddr)
	streamCtx := metadata.NewOutgoingContext(baseCtx, md)

	client := pb_service.NewPushServiceClient(conn)
	stream, err := client.PushStream(streamCtx)
	if err != nil {
		baseCancel()
		conn.Close()
		return nil, err
	}

	pc := &PushClient{
		conn:    conn,
		stream:  stream,
		ctx:     baseCtx,
		cancel:  baseCancel,
		sendCh:  make(chan *pb_service.GateToPush, 100),
		recvCh:  make(chan *pb_service.PushToGate, 100),
		gateSrv: gs,
		clog:    clog,
	}

	// 启动两个协程
	pc.wg.Add(2)
	go pc.sendLoop()
	go pc.recvLoop()

	return pc, nil
}

// sendLoop 发送协程
func (pc *PushClient) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			pc.clog.Error("sendLoop panic", zap.Any("panic", r))
		}
		pc.wg.Done()
	}()

	for {
		select {
		case <-pc.ctx.Done():
			return
		case msg, ok := <-pc.sendCh:
			if !ok {
				return
			}
			if err := pc.stream.Send(msg); err != nil {
				pc.clog.Warn("stream send exit", zap.Error(err))
				return
			}
		}
	}
}

// recvLoop 给Recv增加上下文保护，杜绝永久阻塞
func (pc *PushClient) recvLoop() {
	defer func() {
		if r := recover(); r != nil {
			pc.clog.Error("recvLoop panic", zap.Any("panic", r))
		}
		pc.wg.Done()
	}()

	for {
		select {
		case <-pc.ctx.Done():
			pc.clog.Info("recv loop context canceled, exit")
			return
		default:
		}

		// 异步执行Recv，防止阻塞无法被ctx打断
		recvDone := make(chan error, 1)
		var msg *pb_service.PushToGate
		go func() {
			var err error
			msg, err = pc.stream.Recv()
			recvDone <- err
		}()

		select {
		case <-pc.ctx.Done():
			return
		case err := <-recvDone:
			if err != nil {
				pc.clog.Warn("stream recv exit", zap.Error(err))
				return
			}
			pc.gateSrv.OnPushMessage(msg)
		}
	}
}

// ReportOnline 上报上线
func (pc *PushClient) ReportOnline(uid uint64, address string) {
	select {
	case pc.sendCh <- &pb_service.GateToPush{
		Payload: &pb_service.GateToPush_Online{
			Online: &pb_service.Online{UserId: uid, GateAddress: address},
		},
	}:
	case <-pc.ctx.Done():
	}
}

func (pc *PushClient) ReportOffline(uid uint64, reason string) {
	select {
	case pc.sendCh <- &pb_service.GateToPush{
		Payload: &pb_service.GateToPush_Offline{
			Offline: &pb_service.Offline{UserId: uid, Reason: reason},
		},
	}:
	case <-pc.ctx.Done():
	}
}

func (pc *PushClient) Close() {
	pc.clog.Info("PushClient begin close")
	// 1. 取消上下文，唤醒所有select
	pc.cancel()
	// 2. 关闭发送通道，sendLoop正常退出
	close(pc.sendCh)
	// 3. 等待两个协程全部执行Done
	pc.wg.Wait()
	// 4. 关闭grpc连接
	_ = pc.stream.CloseSend()
	pc.conn.Close()
	pc.clog.Info("PushClient close finish")
}
