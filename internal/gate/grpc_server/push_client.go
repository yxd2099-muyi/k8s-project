package grpc_server

import (
	"context"
	pb_service "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/internal/gate/common"
	"github.com/k8s/muyi/shared/infra/cconst"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"sync"
)

type PushClient struct {
	conn    *grpc.ClientConn
	stream  pb_service.PushService_PushStreamClient
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	sendCh  chan *pb_service.GateToPush // 上报消息
	recvCh  chan *pb_service.PushToGate // 收到的推送
	gateSrv *PushServer
	//address string
}

func NewPushClient(conn *grpc.ClientConn, gs *PushServer) (*PushClient, error) {
	client := pb_service.NewPushServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	argCfg := common.GetArgConfig()
	gateAddr := argCfg.GRpcAddressRegister
	md := metadata.Pairs(cconst.ContextFieldGateAddress, gateAddr)
	ctx = metadata.NewOutgoingContext(context.Background(), md)
	stream, err := client.PushStream(ctx)
	if err != nil {
		conn.Close()
		cancel()
		return nil, err
	}
	pc := &PushClient{
		conn:    conn,
		stream:  stream,
		ctx:     ctx,
		cancel:  cancel,
		sendCh:  make(chan *pb_service.GateToPush, 100),
		recvCh:  make(chan *pb_service.PushToGate, 100),
		gateSrv: gs,
		//address: addr,
	}
	pc.wg.Add(2)
	go pc.sendLoop()
	go pc.recvLoop()
	return pc, nil
}

// sendLoop 从 sendCh 取消息发送
func (pc *PushClient) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
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
				return
			}
		}
	}
}

// recvLoop 接收推送并交给 GateServer
func (pc *PushClient) recvLoop() {
	defer func() {
		if r := recover(); r != nil {
		}
		pc.wg.Done()
	}()
	for {
		msg, err := pc.stream.Recv()
		if err != nil {
			return
		}
		pc.gateSrv.OnPushMessage(msg)
	}
}

// ReportOnline 上报上线
func (pc *PushClient) ReportOnline(uid uint64, address string) {
	pc.sendCh <- &pb_service.GateToPush{
		Payload: &pb_service.GateToPush_Online{
			Online: &pb_service.Online{UserId: uid, GateAddress: address},
		},
	}
}

func (pc *PushClient) ReportOffline(uid uint64, reason string) {
	pc.sendCh <- &pb_service.GateToPush{
		Payload: &pb_service.GateToPush_Offline{
			Offline: &pb_service.Offline{UserId: uid, Reason: reason},
		},
	}
}

func (pc *PushClient) Close() {
	pc.cancel()
	close(pc.sendCh)
	pc.wg.Wait()
	pc.conn.Close()
}
