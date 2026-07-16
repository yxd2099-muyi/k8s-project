package config

import "time"

type RocketMq struct {
	Endpoint     string `mapstructure:"endpoint"`
	Namespace    string `mapstructure:"namespace"`
	AccessKey    string `mapstructure:"accessKey"`
	AccessSecret string `mapstructure:"accessSecret"`
}
type ServerInfo struct {
	ProjectName string `mapstructure:"projectName"`
	ServerId    string `mapstructure:"serverId"`
	Env         string `mapstructure:"env"`
	JwtSecret   string `mapstructure:"jwtSecret"`
}

type Redis struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"poolSize"`
}

type Mysql struct {
	Host                string        `mapstructure:"host"`
	Port                int           `mapstructure:"port"`
	User                string        `mapstructure:"user"`
	Password            string        `mapstructure:"password"`
	DBName              string        `mapstructure:"dbname"`
	MaxIdleConns        int           `mapstructure:"max_idle_conns"`
	MaxOpenConns        int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime     time.Duration `mapstructure:"conn_max_lifetime"`
	IsOpenAutoMigration bool          `mapstructure:"is_open_auto_migration"`
	SlowThreshold       int64         `mapstructure:"slowThreshold"`
	Level               string        `mapstructure:"level"`
}

type Log struct {
	Debug                 bool   `mapstructure:"debug"`
	MaxSize               int    `mapstructure:"maxSize"`
	MaxDays               int    `mapstructure:"maxDays"`
	MaxBackups            int    `mapstructure:"maxBackups"`
	Compress              bool   `mapstructure:"compress"`
	RotateByDay           bool   `mapstructure:"rotateByDay"`
	LogLevel              string `mapstructure:"logLevel"`
	NeedErrLog            bool   `mapstructure:"needErrLog"`
	OtelOpen              bool   `mapstructure:"otelOpen"`
	WriteFile             bool   `mapstructure:"writeFile"`
	TraceExporterEndpoint string `mapstructure:"traceExporterEndpoint"`
	LogExporterEndpoint   string `mapstructure:"logExporterEndpoint"`
}

type Etcd struct {
	Endpoints         []string `mapstructure:"endpoints"`
	DialTimeout       int      `mapstructure:"dialTimeout"`
	LeaseTTL          int      `mapstructure:"ttl"`
	KeepAliveInterval int      `mapstructure:"keepAliveInterval"`
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
}

type BaseConf struct {
	ServerInfo ServerInfo `mapstructure:"serverinfo"`
	Redis      Redis      `mapstructure:"redis"`
	Mysql      Mysql      `mapstructure:"mysql"`
	Log        Log        `mapstructure:"log"`
	Etcd       Etcd       `mapstructure:"etcd"`
	RocketMq   RocketMq   `mapstructure:"rocketmq"`
}
