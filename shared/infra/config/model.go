package config

type Gate struct {
	WsPort   string `mapstructure:"ws_port"`
	GRpcPort string `mapstructure:"grpc_port"`
}
type Web struct {
	Port string `mapstructure:"port"`
}
type Game struct {
	Port string `mapstructure:"port"`
}
