package common

import "github.com/k8s/muyi/shared/infra/config"

type Game struct {
	Port           string `mapstructure:"port"`
	MaxRoomNum     int    `mapstructure:"max_room_num"`
	LogPath        string `mapstructure:"logPath"`
	ErrLogPath     string `mapstructure:"errLogPath"`
	ServiceNameKey string `mapstructure:"serviceNameKey"`
}

type ArgConf struct {
	IPString     string
	PodName      string
	Port         string
	PodIndex     uint32
	GRpcAddr     string
	RegisterAddr string
}
type ServerCfg struct {
	GameServerCfg Game `mapstructure:"game"`
	ArgConf       ArgConf
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
func GetGame() *Game {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.GameServerCfg
}
func GetArg() *ArgConf {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.ArgConf
}
