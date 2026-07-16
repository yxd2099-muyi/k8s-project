package common

import "github.com/k8s/muyi/shared/infra/config"

type Web struct {
	Port           string `mapstructure:"port"`
	LogPath        string `mapstructure:"logPath"`
	ErrLogPath     string `mapstructure:"errLogPath"`
	ServiceNameKey string `mapstructure:"serviceNameKey"`
}

type ServerCfg struct {
	WebServerCfg Web `mapstructure:"web"`
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
func GetWebCfg() *Web {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.WebServerCfg
}
