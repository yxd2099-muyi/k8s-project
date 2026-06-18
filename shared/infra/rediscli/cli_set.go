package rediscli

import "context"

// SAdd set 类型添加操作
func (r *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	res := r.client.SAdd(ctx, key, members...)
	return res.Err()
}
func (r *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	res, err := r.client.SMembers(ctx, key).Result()
	return res, err
}

// SRem 删除元素
func (r *Client) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	res, err := r.client.SRem(ctx, key, members...).Result()
	return res, err
}
