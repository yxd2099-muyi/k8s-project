package cconst

// grpc 之间传递值context 中字段name
const (
	//GRpcContextFieldClientId = "clientId" // 客户端id
	GRpcContextFieldUID      = "UID"      // 用户id
	GRpcContextFieldClientIP = "clientIp" //客户端ip
	GRpcContextFieldGuildId  = "guildId"  //公会Id
)

const (
	ContextFieldRouterTargetAddress = "target-address" // 负载均衡时候 要到的 的目标地址
	ContextFieldRouterXRoomId       = "x-room-id"      // 根据房间负载均衡到 （一致性hash方式） 到具体对应房间
)
