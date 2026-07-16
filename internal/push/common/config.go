package common

import "github.com/k8s/muyi/shared/infra/config"

type Push struct {
	Port           string `mapstructure:"port"`
	LogPath        string `mapstructure:"logPath"`
	ErrLogPath     string `mapstructure:"errLogPath"`
	ServiceNameKey string `mapstructure:"serviceNameKey"`
}
type ArgConf struct {
	IPString     string
	PodName      string
	Port         string
	GRpcAddr     string
	RegisterAddr string
}

type ServerCfg struct {
	Push    Push `mapstructure:"push"`
	ArgConf ArgConf
}

var Conf *config.StaticConfig[ServerCfg]

func InitStaticConfig(basePath, path string) {
	var serverCfg ServerCfg
	c, err := config.LoadConfig(basePath, path, &serverCfg)
	if err != nil {
		panic(err)
	}
	Conf = c
}

func GetBaseCfg() *config.BaseConf {
	if Conf == nil {
		return nil
	}
	return &Conf.Base
}
func GetPush() *Push {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.Push
}
func GetArgCfg() *ArgConf {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.ArgConf
}
