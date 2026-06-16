package config

import (
	"fmt"
	"github.com/k8s/muyi/shared/infra/cconst"
	"os"
	"time"

	"github.com/spf13/viper"
)

var GlobalConf Conf

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

type Conf struct {
	ServerInfo ServerInfo `mapstructure:"serverinfo"`
	Redis      Redis      `mapstructure:"redis"`
	Mysql      Mysql      `mapstructure:"mysql"`
	Log        Log        `mapstructure:"log"`
	Etcd       Etcd       `mapstructure:"etcd"`
	Gate       Gate       `mapstructure:"gate"`
	Game       Game       `mapstructure:"game"`
	Web        Web        `mapstructure:"web"`
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
	Debug       bool   `mapstructure:"debug"`
	MaxSize     int    `mapstructure:"maxSize"`
	MaxDays     int    `mapstructure:"maxDays"`
	MaxBackups  int    `mapstructure:"maxBackups"`
	FileName    string `mapstructure:"fileName"`
	Compress    bool   `mapstructure:"compress"`
	RotateByDay bool   `mapstructure:"rotateByDay"`
	LogLevel    string `mapstructure:"logLevel"`
	NeedErrLog  bool   `mapstructure:"needErrLog"`
	ErrLogPath  string `mapstructure:"errLogPath"`
}

type Etcd struct {
	Endpoints         []string `mapstructure:"endpoints"`
	DialTimeout       int      `mapstructure:"dialTimeout"`
	TTL               int      `mapstructure:"ttl"`
	KeepAliveInterval int      `mapstructure:"keepAliveInterval"`
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
}

// Init 初始化配置入口
func Init() error {
	runEnv := os.Getenv("APP_ENV")
	if runEnv == "" {
		runEnv = EnvLocal
	}
	v := viper.New()

	// 1. 加载toml基础配置
	if runEnv == EnvLocal {
		v.SetConfigFile("configs/config.toml")
	} else {
		// k8s模式读取挂载的configmap配置
		v.SetConfigFile("/app/config/config.toml")
	}
	v.SetConfigType("toml")
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config err: %w", err)
	}

	// 2. k8s环境：读取环境变量覆盖敏感字段
	if runEnv == EnvK8s {
		if val := os.Getenv(EnvJwtSecret); val != "" {
			v.Set("serverinfo.jwtSecret", val)
		}
		if val := os.Getenv(EnvRedisPwd); val != "" {
			v.Set("redis.password", val)
		}
		if val := os.Getenv(EnvMysqlUser); val != "" {
			v.Set("mysql.user", val)
		}
		if val := os.Getenv(EnvMysqlPwd); val != "" {
			v.Set("mysql.password", val)
		}
		if val := os.Getenv(EnvEtcdUser); val != "" {
			v.Set("etcd.username", val)
		}
		if val := os.Getenv(EnvEtcdPwd); val != "" {
			v.Set("etcd.password", val)
		}
	}

	// 3. 反序列化全局配置
	if err := v.Unmarshal(&GlobalConf); err != nil {
		return fmt.Errorf("unmarshal conf err: %w", err)
	}
	return nil
}

func GetEnv() string {
	env := os.Getenv("APP_ENV")
	if env == "" {
		return EnvLocal
	}
	return env
}

func GetGateServerCfg() Gate {
	return GlobalConf.Gate
}
func GetWebServerCfg() Web {
	return GlobalConf.Web
}
func GetGameServerCfg() Game {
	return GlobalConf.Game
}

// GetPodInfo 读取k8s环境变量 POD_NAME POD_IP
func GetPodInfo() (podName, podIP string) {
	podName = os.Getenv(cconst.ENV_POD_NAME)
	podIP = os.Getenv(cconst.ENV_POD_NAME)
	return
}
