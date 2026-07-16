package config

// 环境标识
const (
	EnvLocal = "local"
	EnvK8s   = "k8s"
)

// 所有敏感字段对应环境变量名
const (
	EnvJwtSecret = "JWT_SECRET"
	EnvRedisPwd  = "REDIS_PASSWORD"
	EnvMysqlUser = "MYSQL_USER"
	EnvMysqlPwd  = "MYSQL_PASSWORD"
	EnvEtcdUser  = "ETCD_USER"
	EnvEtcdPwd   = "ETCD_PASSWORD"
)
