package common

type ArgConfig struct {
	IPString string
	Port     string
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
