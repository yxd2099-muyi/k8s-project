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

func GetPushServerInfoTarget(projectName, env, serverType, subType string) string {
	return fmt.Sprintf("%s/%s/%s/%s", projectName, env, serverType, subType) // 注意这里 前面不能加 /
}

func GetEtcdPushServerTarget() string {
	cfg := config.GlobalConf
	serverinfo := cfg.ServerInfo
	project := serverinfo.ProjectName
	env := serverinfo.Env
	target := GetPushServerInfoTarget(project, env, cconst.ServerTypePush, cconst.ServerTypePushSubGRpc)
	return target
}
