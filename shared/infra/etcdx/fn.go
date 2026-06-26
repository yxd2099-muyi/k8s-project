package etcdx

import (
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
)

// GetRoomInfoEtcdPrefixKey 获取房间对应 key 的前缀
// projectName 项目名称  比如这个为k8s-project
// env 环境 eg: dev,prod,k8s
// serverType 服务类型 比如当前是roomServer 对应 room
// subType serverType 的子类型  比如对于gate server 会作为websocket server , 也有可能作为grpc server 那这个值就是grpc
// eg:   k8s-project/dev/gate/ws
// 注意key 最好都是以/  开头 在grpc 时候使用时候避免bug
func GetRoomInfoEtcdPrefixKey(projectName, env, serverType, subType string) string {
	target := GetRoomServerInfoTarget(projectName, env, serverType, subType)
	return fmt.Sprintf("%s/", target)
}

//	func GetRoomInfoEtcdPrefixKeyForEndPoint(projectName, env, serverType, subType string) string {
//		return fmt.Sprintf("/%s/%s/%s/%s", projectName, env, serverType, subType)
//	}
func GetRoomServerInfoTarget(projectName, env, serverType, subType string) string {
	return fmt.Sprintf("/%s/%s/%s/%s", projectName, env, serverType, subType)
}

// GetRoomInfoEtcdKey 获取房间服务etcd 对应的key
// eg: k8s-project/dev/gate/ws/172.16.111.60:9000
func GetRoomInfoEtcdKey(prefix, podAddress string) string {
	return fmt.Sprintf("%s%s", prefix, podAddress)
}

func GetEtcdRoomServerPrefixKey() string {
	cfg := config.GlobalConf
	serverinfo := cfg.ServerInfo
	project := serverinfo.ProjectName
	env := serverinfo.Env
	prefix := GetRoomInfoEtcdPrefixKey(project, env, cconst.ServerTypeRoom, cconst.ServerTypeRoomSubGRpc)
	return prefix
}
func GetEtcdRoomServerTarget() string {
	cfg := config.GlobalConf
	serverinfo := cfg.ServerInfo
	project := serverinfo.ProjectName
	env := serverinfo.Env
	target := GetRoomServerInfoTarget(project, env, cconst.ServerTypeRoom, cconst.ServerTypeRoomSubGRpc)
	return target
}
func GetEtcdRoomServerKey(podAddress string) string {
	prefix := GetEtcdRoomServerPrefixKey()
	key := GetRoomInfoEtcdKey(prefix, podAddress)
	return key
}
