package common

import "github.com/k8s/muyi/shared/infra/config"

type ArgConfig struct {
	GConfig             config.Conf
	IPString            string
	WsPort              string // ws 端口
	GRpcPort            string // grpc 端口
	WsAddress           string //ws 启动地址
	GRpcAddress         string // grpc 启动地址
	WsAddressRegister   string // ws 启动注册地址
	GRpcAddressRegister string // gRpc 启动注册地址
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
