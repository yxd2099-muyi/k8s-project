package etcdapi

import (
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
)

func GetPushServerInfoTarget(projectName, env, serverType, subType string) string {
	return fmt.Sprintf("%s/%s/%s/%s", projectName, env, serverType, subType) // 注意这里 前面不能加 / 后面也不能加 /
}

func GetEtcdPushServerTarget(project, env string) string {
	target := GetPushServerInfoTarget(project, env, cconst.ServerTypePush, cconst.ServerTypePushSubGRpc)
	return target
}
