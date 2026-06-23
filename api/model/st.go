package model

// GameNode game服注册信息，etcd value存json
type GameNode struct {
	PodName    string `json:"pod_name"`
	PodIP      string `json:"pod_ip"`
	GrpcAddr   string `json:"grpc_addr"` // podIP:9000
	RoomMin    uint32 `json:"room_min"`
	RoomMax    uint32 `json:"room_max"`
	MaxRoomCfg uint32 `json:"max_room_cfg"`
}

// GateNode 网关注册信息
type GateNode struct {
	PodName  string `json:"pod_name"`
	PodIP    string `json:"pod_ip"`
	WsAddr   string `json:"ws_addr"`
	GrpcAddr string `json:"grpc_addr"` // game推送gate的grpc地址
}

type RoomMeta struct {
	RoomId     uint32 `json:"room_Id"`     // roomId  room 的房间号
	ServerAddr string `json:"server_addr"` // 房间所在的etcd
}
