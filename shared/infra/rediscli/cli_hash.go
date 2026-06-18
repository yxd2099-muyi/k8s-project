package rediscli

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// ///////////////////hash 操作
func (r *Client) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	return r.client.HIncrBy(ctx, key, field, incr).Result()
}

// HSet msg 可以是一个map 值 也可以是一个 struct
func (r *Client) HSet(ctx context.Context, key string, msg interface{}) (int64, error) {

	return r.client.HSet(ctx, key, msg).Result()
}
func (r *Client) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {

	return r.client.HGetAll(ctx, key)
}

func (r *Client) HSetNX(ctx context.Context, key, field string, value interface{}) (bool, error) {

	return r.client.HSetNX(ctx, key, field, value).Result()
}

const hSetWithExpireScript = `
local key = KEYS[1]
local expire_sec = tonumber(ARGV[1])
-- 如果 expire_sec <= 0，不设过期（或可选 PERSIST）
if expire_sec ~= nil and expire_sec > 0 then
    -- 先 HSET 所有字段
    if #ARGV > 1 then
        redis.call('HSET', key, unpack(ARGV, 2, #ARGV))
    end
    redis.call('EXPIRE', key, expire_sec)
end
return redis.status_reply('OK')
`

func (r *Client) HSetWithExpireForMap(ctx context.Context, key string, msg map[string]interface{}, expiration time.Duration) error {
	if len(msg) == 0 {
		return errors.New("msg map is empty")
	}
	if expiration < 0 {
		return errors.New("expiration must be >= 0")
	}
	args := make([]interface{}, 0, len(msg)*2+1)
	args = append(args, int64(expiration.Seconds())) // ARGV[1]
	for k, v := range msg {
		args = append(args, k, v)
	}

	_, err := r.client.Eval(ctx, hSetWithExpireScript, []string{key}, args...).Result()
	return err
}
