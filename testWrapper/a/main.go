package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	pb_room "github.com/k8s/muyi/api/pb/room"
	"google.golang.org/protobuf/proto"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	pb_base "github.com/k8s/muyi/api/pb/base"
)

const (
	// Gate网关地址
	gateHost = "172.16.111.60:2200"
	//gateHost = "127.0.0.1:8090"
	wsPath = "/ws"
	// 读写超时
	writeTimeout = 5 * time.Second
	readTimeout  = 30 * time.Second
	pingInterval = 10 * time.Second
	reqTimeout   = 5 * time.Second
)

// WsClient 网关websocket客户端
type WsClient struct {
	ctx        context.Context
	cancel     context.CancelFunc
	conn       *websocket.Conn
	writeChan  chan []byte // 写队列，并发安全
	reqMap     sync.Map    // seq -> chan *pb_base.WsFrame 等待响应
	seqCounter uint64      // 自增序列号
	closeFlag  atomic.Bool
	wg         sync.WaitGroup

	UID uint64 // 当前登录用户ID
}

// NewWsClient 新建连接，uid为当前用户ID
func NewWsClient(uid uint64) (*WsClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// 拼接ws地址，网关通过url参数获取uid
	u := url.URL{
		Scheme: "ws",
		Host:   gateHost,
		Path:   wsPath,
	}
	query := u.Query()
	query.Set("uid", fmt.Sprintf("%d", uid))
	u.RawQuery = query.Encode()

	// 建立ws连接
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dial gate failed: %w", err)
	}

	cli := &WsClient{
		ctx:       ctx,
		cancel:    cancel,
		conn:      conn,
		writeChan: make(chan []byte, 256),
		UID:       uid,
	}

	// 启动读写协程
	cli.wg.Add(2)
	go cli.readLoop()
	go cli.writeLoop()

	fmt.Printf("[Client] connect gate success, uid=%d\n", uid)
	return cli, nil
}

// SendRequest 通用发送请求方法
// firstKind: 业务大类 FIRST_ROOM / FIRST_GUILD
// cmd: CmdKind 业务指令
// roomId: 房间ID
// bizPayload: 业务自定义参数二进制
// 返回顶层WsFrame响应
func (c *WsClient) SendRequest(
	firstKind pb_base.FirstKind,
	cmd pb_room.CmdRoomKind,
	roomId uint32,
	bizPayload []byte,
) (*pb_base.WsFrame, error) {
	if c.closeFlag.Load() {
		return nil, errors.New("client closed")
	}

	// 1. 生成seq
	seq := atomic.AddUint64(&c.seqCounter, 1)
	respChan := make(chan *pb_base.WsFrame, 1)
	c.reqMap.Store(seq, respChan)
	defer c.reqMap.Delete(seq)

	// 2. 组装内层 ReqBody
	reqBody := &pb_base.ReqBody{
		Cmd:     uint32(cmd),
		Uid:     c.UID,
		RoomId:  roomId,
		Payload: bizPayload,
	}
	reqBin, err := proto.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal ReqBody err: %w", err)
	}

	// 3. 组装外层 WsFrame
	nowTs := time.Now().UnixMilli()
	frame := &pb_base.WsFrame{
		FrameType: pb_base.FrameType_FRAME_REQUEST,
		FirstKind: firstKind,
		Seq:       seq,
		Uid:       c.UID,
		Timestamp: nowTs,
		RoomId:    roomId,
		Payload:   reqBin,
	}
	frameBin, err := proto.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("marshal WsFrame err: %w", err)
	}

	// 4. 写入发送队列
	select {
	case c.writeChan <- frameBin:
	case <-time.After(writeTimeout):
		return nil, errors.New("send queue full timeout")
	case <-c.ctx.Done():
		return nil, errors.New("client closed")
	}

	fmt.Printf("[SendRequest] seq=%d firstKind=%d cmd=%d roomId=%d\n", seq, firstKind, cmd, roomId)

	// 5. 阻塞等待响应
	select {
	case respFrame := <-respChan:
		return respFrame, nil
	case <-time.After(reqTimeout):
		return nil, fmt.Errorf("request seq=%d timeout", seq)
	case <-c.ctx.Done():
		return nil, errors.New("client closed")
	}
}

// SendCreateRoom 快捷封装：发送创建房间请求
func (c *WsClient) SendCreateRoom(roomId uint32, bizArgs []byte) (*pb_base.WsFrame, error) {
	return c.SendRequest(
		pb_base.FirstKind_FIRST_ROOM,
		//pb_room.CmdRoomKind_Cmd_ROOM_UNKNOW,
		pb_room.CmdRoomKind_CMD_ROOM_CREATE,
		roomId,
		bizArgs,
	)
}

// readLoop 读协程：接收响应、推送、ping/pong
func (c *WsClient) readLoop() {
	defer func() {
		c.cancel()
		c.wg.Done()
		fmt.Println("[ReadLoop] exit")
	}()

	// pong超时回调
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})
	_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msgType, data, err := c.conn.ReadMessage()
			if err != nil {
				fmt.Printf("[ReadLoop] read err: %v\n", err)
				return
			}

			switch msgType {
			case websocket.BinaryMessage:
				c.handleFrame(data)
			case websocket.PingMessage:
				// 回复pong保活
				_ = c.conn.WriteMessage(websocket.PongMessage, nil)
			case websocket.CloseMessage:
				fmt.Println("[ReadLoop] gate close connection")
				return
			}
		}
	}
}

// handleFrame 解析二进制WsFrame，分发响应/推送
func (c *WsClient) handleFrame(data []byte) {
	var frame pb_base.WsFrame
	if err := proto.Unmarshal(data, &frame); err != nil {
		fmt.Printf("[HandleFrame] unmarshal WsFrame err: %v\n", err)
		return
	}
	fmt.Println("[HandleFrame] handleFrame ", frame.FrameType)
	switch frame.FrameType {
	case pb_base.FrameType_FRAME_RESPONSE:
		// 请求响应，匹配seq唤醒等待chan
		val, ok := c.reqMap.Load(frame.Seq)
		if !ok {
			fmt.Printf("[Response] unknown seq=%d, drop\n", frame.Seq)
			return
		}
		respChan := val.(chan *pb_base.WsFrame)
		respChan <- &frame

		// 解析RespBody
		var respBody pb_base.RespBody
		_ = proto.Unmarshal(frame.Payload, &respBody)
		fmt.Printf("[Response] seq=%d code=%d msg=%s payloadLen=%d\n",
			frame.Seq, frame.ErrCode, frame.ErrMsg, len(respBody.Payload))

	case pb_base.FrameType_FRAME_PUSH:
		// 服务主动推送，解析PushBody
		var pushBody pb_base.PushBody
		_ = proto.Unmarshal(frame.Payload, &pushBody)
		fmt.Printf("[PushNotify] uid=%d roomId=%d pushCmd=%d payloadLen=%d\n",
			frame.Uid, frame.RoomId, pushBody.Cmd, len(pushBody.Payload))

	default:
		fmt.Printf("[UnknownFrame] type=%d\n", frame.FrameType)
	}
}

// writeLoop 写协程：统一发包 + 定时ping探活
func (c *WsClient) writeLoop() {
	defer func() {
		_ = c.conn.Close()
		c.wg.Done()
		fmt.Println("[WriteLoop] exit")
	}()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// 发送ping保活
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case msgBin := <-c.writeChan:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.BinaryMessage, msgBin); err != nil {
				fmt.Printf("[WriteLoop] send err: %v\n", err)
				return
			}
		}
	}
}

// Close 优雅关闭客户端
func (c *WsClient) Close() {
	if c.closeFlag.Swap(true) {
		return
	}
	c.cancel()
	// 发送关闭帧
	_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	c.wg.Wait()
	fmt.Println("[Client] close complete")
}

func main() {
	// 模拟用户uid
	uid := uint64(10001)
	fmt.Println("start ,", uid)
	client, err := NewWsClient(uid)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// 示例1：发送创建房间请求，房间号100001，无业务参数
	resp, err := client.SendCreateRoom(19, []byte{})
	if err != nil {
		fmt.Println("send create room failed:", err)
		return
	}

	// 判断返回结果
	if resp.ErrCode == pb_base.ErrCode_EC_OK {
		fmt.Println("==== 创建房间成功 ====")
	} else {
		fmt.Printf("==== 创建房间失败 code=%d msg=%s ====\n", resp.ErrCode, resp.ErrMsg)
	}

	// 阻塞等待服务端主动推送消息
	fmt.Println("\nWaiting server push message, Ctrl+C exit")
	select {}
}
