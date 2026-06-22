package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/k8s/muyi/api/model"
	"github.com/k8s/muyi/shared/infra/cconst"
	"github.com/k8s/muyi/shared/infra/rediscli"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"time"
)

type User struct {
	rc *rediscli.Client
}

func NewUserObj() *User {
	obj := &User{}
	obj.rc = rediscli.GetClient()
	return obj
}
func (r *User) DelUserSession(ctx context.Context, uId uint64) {
	key := cconst.RedisKeyForUserSession(uId)
	r.rc.Delete(ctx, key)
}
func (r *User) SetUserSession(ctx context.Context, clog *zap.Logger, uId uint64, session *model.UserSession, expire time.Duration) error {
	key := cconst.RedisKeyForUserSession(uId)
	err := r.rc.PipelineHashSetExec(ctx, key, expire, session)
	if err != nil {
		clog.Error("pipeline hash set failed", zap.Error(err))
	}
	return err
}
func (r *User) GetUserSession(ctx context.Context, clog *zap.Logger, uId uint64) (*model.UserSession, error) {
	key := cconst.RedisKeyForUserSession(uId)
	cmd := r.rc.HGetAll(ctx, key)
	if err := cmd.Err(); err != nil {
		clog.Error("Get user session error", zap.Error(err))
		return nil, fmt.Errorf("hgetall failed: %w", err)
	}
	dataMap := cmd.Val()
	if len(dataMap) == 0 {
		return nil, nil // 约定：nil session代表会话不存在，无错误
	}
	var session model.UserSession
	if err := cmd.Scan(&session); err != nil {
		clog.Error("Get user session error", zap.Error(err))
		return nil, fmt.Errorf("hscan failed: %w", err)
	}
	return &session, nil
}
func (r *User) GetSomeUsersSession(ctx context.Context, clog *zap.Logger, uids []uint64) ([]*model.UserSession, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	pipe := r.rc.Pipeline()
	futures := make([]*redis.MapStringStringCmd, len(uids))
	for i, uid := range uids {
		key := fmt.Sprintf("hash:user:session:%d", uid)
		futures[i] = pipe.HGetAll(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && errors.Is(err, redis.Nil) {
		return nil, err
	}
	result := make([]*model.UserSession, 0, len(uids))
	for _, future := range futures {
		err = future.Err()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// key 不存在，跳过或记录日志
				continue
			}
			clog.Error("GetSomeUsersSession", zap.Error(err))
			continue
		}

		var u model.UserSession
		err = future.Scan(&u)
		if err != nil {
			clog.Error("GetSomeUsersSession", zap.Error(err))
			continue
		}
		result = append(result, &u)
	}
	clog.Info("GetSomeUsersSession result", zap.Any("result", result))
	return result, nil
}
