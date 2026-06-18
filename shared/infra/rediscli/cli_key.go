package rediscli

import (
	"context"
	"time"
)

/*
*
针对所有key
*/
func (r *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.client.Expire(ctx, key, expiration).Err()
}

// 删除指定key
func (r *Client) Delete(ctx context.Context, key string) {
	r.client.Del(ctx, key)
}
func (r *Client) Exist(ctx context.Context, key string) (bool, error) {

	exists, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if exists == 1 {
		return true, nil
	}
	return false, nil
}
