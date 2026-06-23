package common

type ArgConfig struct {
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
	ArgConfigG = c
	return
}

func GetArgConfig() *ArgConfig {
	return ArgConfigG
}
