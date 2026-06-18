package rediscli

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

// SAdd set 类型添加操作
func (r *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {

	res := r.client.ZAdd(ctx, key, members...)
	return res.Err()
}
func (r *Client) ZRem(ctx context.Context, key string, members ...interface{}) error {

	res := r.client.ZRem(ctx, key, members...)
	return res.Err()
}
func (r *Client) ZRemRangeByScoreLessThan(ctx context.Context, key string, threshold float64) error {

	maxV := fmt.Sprintf("(%f", threshold)
	res := r.client.ZRemRangeByScore(ctx, key, "-inf", maxV)
	return res.Err()
}
func (r *Client) ZCard(ctx context.Context, key string) (int64, error) {

	res := r.client.ZCard(ctx, key)
	return res.Val(), res.Err()
}
func (r *Client) ZScore(ctx context.Context, key, member string) (float64, error) {

	res := r.client.ZScore(ctx, key, member)
	return res.Val(), res.Err()
}

// 降序排列
func (r *Client) ZRevRank(ctx context.Context, key string, member string) (int64, error) {

	res := r.client.ZRevRank(ctx, key, member)
	return res.Val(), res.Err()
}
func (r *Client) ZRank(ctx context.Context, key string, member string) (int64, error) {

	res := r.client.ZRank(ctx, key, member)
	return res.Val(), res.Err()
}
func (r *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {

	res := r.client.ZRevRange(ctx, key, start, stop)
	return res.Val(), res.Err()
}
func (r *Client) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {

	res, err := r.client.ZRevRangeWithScores(ctx, key, start, stop).Result()
	return res, err
}
func (r *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {

	res := r.client.ZRange(ctx, key, start, stop)
	return res.Val(), res.Err()
}
func (r *Client) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) (uint64, []string, error) {

	keys, cur, err := r.client.ZScan(ctx, key, cursor, match, count).Result()
	return cur, keys, err
}
