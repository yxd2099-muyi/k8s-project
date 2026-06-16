package config

type Gate struct {
	WsPort       string `mapstructure:"ws_port"`
	GRpcPort     string `mapstructure:"grpc_port"`
	RedisExpire  int    `mapstructure:"redis_expire"`
	SyncInterval int    `mapstructure:"sync_interval"`
	PingInterval int    `mapstructure:"ping_interval"`
	PongTimeout  int    `mapstructure:"pong_timeout"`
	GrpcPoolSize int    `mapstructure:"grpc_pool_size"`
}
type Web struct {
	Port string `mapstructure:"port"`
}
type Game struct {
	Port string `mapstructure:"port"`
}
