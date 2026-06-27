package cconst

import "fmt"

//// GetRoomInfoEtcdPrefixKey 获取房间对应 key 的前缀
//// projectName 项目名称  比如这个为k8s-project
//// env 环境 eg: dev,prod,k8s
//// serverType 服务类型 比如当前是roomServer 对应 room
//// subType serverType 的子类型  比如对于gate server 会作为websocket server , 也有可能作为grpc server 那这个值就是grpc
//// eg:   k8s-project/dev/gate/ws
//func GetRoomInfoEtcdPrefixKey(projectName, env, serverType, subType string) string {
//	return fmt.Sprintf("%s/%s/%s/%s/", projectName, env, serverType, subType)
//}
//
//// GetRoomInfoEtcdKey 获取房间服务etcd 对应的key
//// eg: k8s-project/dev/gate/ws/172.16.111.60:9000
//func GetRoomInfoEtcdKey(prefix, podAddress string) string {
//	return fmt.Sprintf("%s%s", prefix, podAddress)
//}

//const EtcdGRpcPrefix = "etcd://"

const EtcdGRpcPrefix = "etcd:///"

// GetGrpcEtcdClientTarget
// target 也就是service 必须是/ 开头  /dev/game
func GetGRpcEtcdClientTarget(target string) string {
	target = fmt.Sprintf("%s%s", EtcdGRpcPrefix, target)
	return target
}
