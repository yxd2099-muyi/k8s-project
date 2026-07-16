package common

import "github.com/k8s/muyi/shared/infra/config"

type Gate struct {
	WsPort         string `mapstructure:"ws_port"`
	GRpcPort       string `mapstructure:"grpc_port"`
	RedisExpire    int    `mapstructure:"redis_expire"`
	SyncInterval   int    `mapstructure:"sync_interval"`
	PingInterval   int    `mapstructure:"ping_interval"`
	PongTimeout    int    `mapstructure:"pong_timeout"`
	GrpcPoolSize   int    `mapstructure:"grpc_pool_size"`
	LogPath        string `mapstructure:"logPath"`
	ErrLogPath     string `mapstructure:"errLogPath"`
	ServiceNameKey string `mapstructure:"serviceNameKey"`
}
type ArgConf struct {
	IPString            string
	WsPort              string // ws 端口
	GRpcPort            string // grpc 端口
	WsAddress           string //ws 启动地址
	GRpcAddress         string // grpc 启动地址
	WsAddressRegister   string // ws 启动注册地址
	GRpcAddressRegister string // gRpc 启动注册地址
}
type ServerCfg struct {
	GateServerCfg Gate `mapstructure:"gate"`
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
func GetGate() *Gate {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.GateServerCfg
}
func GetGateArg() *ArgConf {
	if Conf == nil {
		return nil
	}
	return &Conf.ServerCfg.ArgConf
}
