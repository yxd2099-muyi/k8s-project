package common

import "github.com/k8s/muyi/shared/infra/config"

type ArgConfig struct {
	GConfig      config.Conf
	IPString     string
	PodName      string
	Port         string
	PodIndex     uint32
	GRpcAddr     string
	RegisterAddr string
}

var ArgConfigG *ArgConfig

func InitArgConfig() {
	c := &ArgConfig{}
	c.GConfig = config.GetConfig()
	ArgConfigG = c
	return
}

func GetArgConfig() *ArgConfig {
	return ArgConfigG
}
