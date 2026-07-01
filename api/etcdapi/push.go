package etcdapi

import (
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
)

func GetPushServerInfoTarget(projectName, env, serverType, subType string) string {
	return fmt.Sprintf("%s/%s/%s/%s", projectName, env, serverType, subType) // 注意这里 前面不能加 / 后面也不能加 /
}

func GetEtcdPushServerTarget() string {
	cfg := config.GlobalConf
	serverinfo := cfg.ServerInfo
	project := serverinfo.ProjectName
	env := serverinfo.Env
	target := GetPushServerInfoTarget(project, env, cconst.ServerTypePush, cconst.ServerTypePushSubGRpc)
	return target
}
