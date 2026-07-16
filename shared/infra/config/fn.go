package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
)

// path 可以是自己配置的， 也可以是k8s模式读取挂载的configmap配置
func initCfg(path string, obj any, baseFlag bool) error {
	runEnv := os.Getenv("APP_ENV")
	if runEnv == "" {
		runEnv = EnvLocal
	}
	v := viper.New()
	v.SetConfigFile(path)
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
	if err := v.Unmarshal(obj); err != nil {
		return fmt.Errorf("unmarshal conf err: %w", err)
	}
	return nil
}

func InitConfig(basePath, path string, base, serverCfg any) error {
	err := initCfg(basePath, base, true)
	if err != nil {
		return err
	}
	err = initCfg(path, serverCfg, false)
	if err != nil {
		return err
	}
	return nil
}

// StaticConfig 保留 Base 字段，ServerCfg 由泛型参数 T 决定
type StaticConfig[T any] struct {
	Base      BaseConf
	ServerCfg T
}

// LoadConfig 加载通用配置和业务配置，返回 *StaticConfig[T]
func LoadConfig[T any](basePath, path string, serverCfg *T) (*StaticConfig[T], error) {
	var base BaseConf
	// 假设 InitConfig 能同时填充 base 和 serverCfg
	err := InitConfig(basePath, path, &base, serverCfg)
	if err != nil {
		return nil, err
	}
	return &StaticConfig[T]{
		Base:      base,
		ServerCfg: *serverCfg,
	}, nil
}

func GetEnv() string {
	env := os.Getenv("APP_ENV")
	if env == "" {
		return EnvLocal
	}
	return env
}
