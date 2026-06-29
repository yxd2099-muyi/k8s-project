package grpc_server

import (
	"context"
	pb "github.com/k8s/muyi/api/pb/service"
	"github.com/k8s/muyi/shared/infra/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"log"
	"runtime/debug"
	"sync"
)

type PushServer struct {
	pb.UnimplementedPushServiceServer
	ctx         context.Context
	cancel      context.CancelFunc
	pushQueue   chan *PushTask
	uidToGate   sync.Map // uint64 -> string gate_address
	gateStreams sync.Map // string gate_address -> PushService_PushStreamServer
	clog        *zap.Logger
}

type PushTask struct {
	EventID string
	UIDs    []uint64
	Payload []byte
}

func NewPushServer() *PushServer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &PushServer{
		ctx:       ctx,
		cancel:    cancel,
		pushQueue: make(chan *PushTask, 50000),
		clog:      logger.L,
	}
	s.startWorkers(10)
	return s
}

func (s *PushServer) SendPushEvents(stream pb.PushService_SendPushEventsServer) error {
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}
		s.pushQueue <- &PushTask{
			EventID: event.EventId,
			UIDs:    event.Uids,
			Payload: event.Payload,
		}
	}
}

func (s *PushServer) startWorkers(n int) {
	for i := 0; i < n; i++ {
		go s.worker(i)
	}
}

func (s *PushServer) worker(id int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PushServer Worker %d] panic recovered: %v\n%s", id, r, debug.Stack())
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.pushQueue:
			s.dispatch(task)
		}
	}
}

// 支持批量，按 Gate 分组发送
func (s *PushServer) dispatch(task *PushTask) {
	groups := s.groupByGate(task.UIDs)
	for gateAddr, uids := range groups {
		if stream, ok := s.gateStreams.Load(gateAddr); ok {
			if err := stream.(pb.PushService_PushStreamServer).Send(&pb.PushToGate{
				EventId: task.EventID,
				Uids:    uids,
				Payload: task.Payload,
			}); err != nil {
				log.Printf("dispatch to gate %s failed", gateAddr)
			}
		}
	}
}

// 将要推送的用户按他们所在的 GateServer 地址分组，方便批量下发，减少网络调用次数
func (s *PushServer) groupByGate(uids []uint64) map[string][]uint64 {
	groups := make(map[string][]uint64)

	for _, uid := range uids {
		if gateAddr, ok := s.uidToGate.Load(uid); ok {
			ga := gateAddr.(string)
			groups[ga] = append(groups[ga], uid)
		} else {
			// 可记录未找到对应 Gate 的用户（离线或路由丢失）
			log.Printf("user %d has no gate address", uid)
		}
	}

	return groups
}

// 推荐方式：通过 gRPC Metadata 传递 gate_address 这个需要验证 TODO
func extractGateAddress(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "unknown-gate"
	}

	values := md.Get("gate-address")
	if len(values) > 0 {
		return values[0]
	}

	// 备选：从 peer 信息获取（IP:Port）
	if peerInfo, ok := peer.FromContext(ctx); ok {
		return peerInfo.Addr.String()
	}

	return "unknown-gate"
}

// 双向流（Gate 向所有 PushServer 上报）
func (s *PushServer) PushStream(stream pb.PushService_PushStreamServer) error {
	gateAddr := extractGateAddress(stream.Context())
	s.gateStreams.Store(gateAddr, stream)
	defer s.gateStreams.Delete(gateAddr)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PushStream] panic recovered: %v", r)
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			msg, err := stream.Recv()
			if err != nil {
				return err
			}
			switch v := msg.Payload.(type) {
			case *pb.GateToPush_Online:
				s.uidToGate.Store(v.Online.UserId, v.Online.GateAddress)
			case *pb.GateToPush_Offline:
				s.uidToGate.Delete(v.Offline.UserId)
			case *pb.GateToPush_Ack:
				// 处理 ACK
			}
		}
	}
}

func (s *PushServer) Shutdown() {
	s.cancel()
}
