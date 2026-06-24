package common

import "github.com/k8s/muyi/shared/infra/config"

type ArgConfig struct {
	GConfig  config.Conf
	IPString string
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
