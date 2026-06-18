package rediscli

import "context"

// list  类型添加操作
func (r *Client) LPush(ctx context.Context, key string, members ...interface{}) error {
	res := r.client.LPush(ctx, key, members...)
	return res.Err()
}
func (r *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	res := r.client.LRange(ctx, key, start, stop)
	return res.Result()
}
