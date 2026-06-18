package rediscli

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// Set 设置键值对
func (r *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Set 设置过期时间
func (r *Client) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) error {

	return r.client.SetEx(ctx, key, value, expiration).Err()
}

// Get 获取键值
func (r *Client) Get(ctx context.Context, key string) (string, error) {

	return r.client.Get(ctx, key).Result()
}

// 获取int64值
func (r *Client) GetInt64(ctx context.Context, key string) (int64, error) {

	return r.client.Get(ctx, key).Int64()
}

// GetWithDefault 获取键值，如果不存在则返回默认值
func (r *Client) GetWithDefault(ctx context.Context, key, defaultValue string) (string, error) {

	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return defaultValue, nil
	}
	return val, err
}

// SetNX 设置键值对，仅当键不存在时。 原子操作
func (r *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {

	return r.client.SetNX(ctx, key, value, expiration).Result()
}

// MSet 批量设置键值对
func (r *Client) MSet(ctx context.Context, pairs ...interface{}) error {

	return r.client.MSet(ctx, pairs...).Err()
}

// MGet 批量获取键值
func (r *Client) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {

	return r.client.MGet(ctx, keys...).Result()
}

// Incr 将键的值增加1
func (r *Client) Incr(ctx context.Context, key string) (int64, error) {
	return r.client.Incr(ctx, key).Result()
}

// Incr 将键的值减少 1
func (r *Client) Decr(ctx context.Context, key string) (int64, error) {
	return r.client.Decr(ctx, key).Result()
}

// ConfigGet 获取配置
func (r *Client) ConfigGet(ctx context.Context, key string) (map[string]string, error) {
	v, err := r.client.ConfigGet(ctx, key).Result()
	return v, err
}
