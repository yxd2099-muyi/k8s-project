package common

import (
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/config"
)

func GetEtcdRoomServerKey() string {
	prefix := GetEtcdRoomServerPrefixKey()
	argCfg := GetArgConfig()
	key := cconst.GetRoomInfoEtcdKey(prefix, argCfg.RegisterAddr)
	return key
}
func GetEtcdRoomServerPrefixKey() string {
	cfg := config.GlobalConf
	serverinfo := cfg.ServerInfo
	project := serverinfo.ProjectName
	env := serverinfo.Env
	prefix := cconst.GetRoomInfoEtcdPrefixKey(project, env, cconst.ServerTypeRoom, cconst.ServerTypeRoomSubGRpc)
	return prefix
}
