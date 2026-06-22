package common

type ArgConfig struct {
	IPString string
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
