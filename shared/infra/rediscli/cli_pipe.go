package rediscli

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func (r *Client) Pipeline() redis.Pipeliner {
	p := r.client.Pipeline()
	return p
}
func (r *Client) PipelineExec(ctx context.Context, p redis.Pipeliner) error {
	_, err := p.Exec(ctx)
	return err
}
func (r *Client) PipelineHashSetExec(ctx context.Context, key string, expire time.Duration, values ...interface{}) error {
	p := r.client.Pipeline()
	p.HSet(ctx, key, values...)
	if expire > 0 {
		p.Expire(ctx, key, expire)
	}
	_, err := p.Exec(ctx)
	return err
}
func (r *Client) PipelineHashSetMapInfo(ctx context.Context, key string, p redis.Pipeliner, m map[string]interface{}, expire time.Duration) {

	if len(m) == 0 {
		return
	}
	for k, v := range m {
		p.HSet(ctx, key, k, v)
	}
	if expire > 0 {
		p.Expire(ctx, key, expire)
	}
	return
}
func (r *Client) PipelineHashSetStructInfo(ctx context.Context, key string, p redis.Pipeliner, expire time.Duration, values ...interface{}) {

	if len(values) == 0 {
		return
	}
	p.HSet(ctx, key, values...)
	if expire > 0 {
		p.Expire(ctx, key, expire)
	}
	return
}

// 存在就不设置， 不存在就设置
func (r *Client) PipelineHashSetNXExec(ctx context.Context, key string, m map[string]interface{}, expire time.Duration) error {

	p := r.client.Pipeline()
	if len(m) == 0 {
		return fmt.Errorf("value is empty")
	}
	for k, v := range m {
		p.HSetNX(ctx, key, k, v)
	}
	if expire > 0 {
		p.Expire(ctx, key, expire)
	}
	_, err := p.Exec(ctx)
	return err
}
func (r *Client) PipeLineZSetExpireExec(ctx context.Context, key string, expire time.Duration, zs ...redis.Z) error {
	p := r.client.Pipeline()
	p.ZAdd(ctx, key, zs...)
	if expire > 0 {
		p.Expire(ctx, key, expire)
	}
	_, err := p.Exec(ctx)
	return err
}
