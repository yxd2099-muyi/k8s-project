package rediscli

import (
	"context"
	"fmt"
	"github.com/k8s/muyi/shared/infra/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"sync"
)

type Client struct {
	client *redis.Client
	clog   *zap.Logger
}

var (
	redisClient *Client
	clientOnce  sync.Once
)

func GetClient() *Client {
	clientOnce.Do(func() {
		redisClient = &Client{}
	})
	return redisClient
}
func (r *Client) Init(clog *zap.Logger, cfg *config.Redis) error {
	if cfg == nil {
		return fmt.Errorf("redis config nil")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + cfg.Port,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return err
	}
	r.client = client
	r.clog = clog
	return nil
}
func (r *Client) Close() {
	r.clog.Info("close redis client")
	err := r.client.Close()
	if err != nil {
		r.clog.Error("close redis client", zap.Error(err))
	}
}
func (r *Client) GetNativeClient() *redis.Client {
	return r.client
}
